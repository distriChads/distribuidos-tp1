package top_five_country_budget

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type TopFiveCountryBudgetConfig struct {
	worker.WorkerConfig
}

type TopFiveCountryBudget struct {
	worker.Worker
	top_five               map[string][]CountrByBudget
	messages_before_commit int
	storage_base_dir       string
}

type CountrByBudget struct {
	Country string
	Budget  int
}

var log = logging.MustGetLogger("top_five_country_budget")

func (g *TopFiveCountryBudget) EnsureClient(client_id string) {
	if _, ok := g.top_five[client_id]; !ok {
		g.top_five[client_id] = make([]CountrByBudget, 0)
	}
}

func (g *TopFiveCountryBudget) HandleCommit(client_id string, message amqp091.Delivery) error {

	storeGroupedElements(g.top_five[client_id], client_id)
	message.Ack(false)
	return nil
}

func (g *TopFiveCountryBudget) MapToLines(client_id string) string {
	return mapToLines(g.top_five[client_id])
}

func mapToLines(top_five []CountrByBudget) string {
	var lines []string
	for _, country_in_top := range top_five {
		line := fmt.Sprintf("%s%s%d", country_in_top.Country, worker.MESSAGE_SEPARATOR, country_in_top.Budget)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *TopFiveCountryBudget) HandleEOF(client_id string, message_id string) error {
	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	delete(g.top_five, client_id)
	return nil
}

func (g *TopFiveCountryBudget) UpdateState(lines []string, client_id string, message_id string) {
	g.top_five[client_id] = updateTopFive(lines, g.top_five[client_id])
}

// ---------------------------------
// MESSAGE FORMAT: COUNTRY|BUDGET
// ---------------------------------
const COUNTRY = 0
const BUDGET = 1

func updateTopFive(lines []string, top_five []CountrByBudget) []CountrByBudget {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		country := parts[COUNTRY]
		budget, err := strconv.Atoi(parts[BUDGET])
		if err != nil {
			continue
		}
		if len(top_five) < 5 {
			top_five = append(top_five, CountrByBudget{Country: country, Budget: budget})
			sort.Slice(top_five, func(i, j int) bool {
				return top_five[i].Budget > top_five[j].Budget
			})
		} else {
			if top_five[4].Budget < budget {
				top_five[4] = CountrByBudget{Country: country, Budget: budget}
				sort.Slice(top_five, func(i, j int) bool {
					return top_five[i].Budget > top_five[j].Budget
				})
			}
		}

	}

	return top_five
}

func storeGroupedElements(results []CountrByBudget, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() []CountrByBudget {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return []CountrByBudget{}
}

func NewTopFiveCountryBudget(config TopFiveCountryBudgetConfig, messages_before_commit int, storage_base_dir string) *TopFiveCountryBudget {
	log.Infof("TopFiveCountryBudget: %+v", config)
	worker, err := worker.NewWorker(config.WorkerConfig, 1)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &TopFiveCountryBudget{
		Worker:                 *worker,
		top_five:               make(map[string][]CountrByBudget, 0),
		messages_before_commit: messages_before_commit,
		storage_base_dir:       storage_base_dir,
	}
}
