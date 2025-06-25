package top_five_country_budget

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type TopFiveCountryBudgetConfig struct {
	worker.WorkerConfig
}

type TopFiveCountryBudget struct {
	worker.Worker
	top_five         map[string]map[string][]CountrByBudget
	storage_base_dir string
	eof_id           map[string]string
	node_id          map[string]string
}

type CountrByBudget struct {
	Country string
	Budget  int
}

var log = logging.MustGetLogger("top_five_country_budget")

func (g *TopFiveCountryBudget) EnsureClient(client_id string) {
	if _, ok := g.top_five[client_id]; !ok {
		g.top_five[client_id] = make(map[string][]CountrByBudget, 0)
	}
	if _, ok := g.top_five[client_id][client_id]; !ok {
		g.top_five[client_id][client_id] = make([]CountrByBudget, 0)
	}

	if _, ok := g.eof_id[client_id]; !ok {
		ids_to_append := make([]string, 2)
		message_id, err := uuid.NewRandom()
		if err != nil {
			log.Errorf("Error generating uuid: %s", err.Error())
			return
		}
		ids_to_append[0] = message_id.String()
		g.eof_id[client_id] = message_id.String()
		message_id, err = uuid.NewRandom()
		if err != nil {
			log.Errorf("Error generating uuid: %s", err.Error())
			return
		}
		ids_to_append[1] = message_id.String()
		g.node_id[client_id] = message_id.String()
		common_statefull_worker.StoreMyId(g.storage_base_dir, ids_to_append, client_id)
	}
}

func (g *TopFiveCountryBudget) HandleCommit(client_id string, message amqp091.Delivery) error {

	err := common_statefull_worker.StoreElements(g.top_five[client_id], client_id, g.storage_base_dir)
	if err != nil {
		return err
	}
	message.Ack(false)
	return nil
}

func (g *TopFiveCountryBudget) MapToLines(client_id string) string {
	return mapToLines(g.top_five[client_id][client_id])
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
	err := common_statefull_worker.SendResult(g.Worker, client_id, g.MapToLines(client_id), g.node_id[client_id], g.eof_id[client_id])
	if err != nil {
		return err
	}
	delete(g.top_five, client_id)
	common_statefull_worker.CleanTopNNode(g.storage_base_dir, client_id)
	return nil
}

func (g *TopFiveCountryBudget) UpdateState(lines []string, client_id string, message_id string) bool {
	g.top_five[client_id][client_id] = updateTopFive(lines, g.top_five[client_id][client_id])
	return false
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
		new_country_budget := CountrByBudget{Country: country, Budget: budget}
		if slices.Contains(top_five, new_country_budget) {
			continue
		}
		if len(top_five) < 5 {
			top_five = append(top_five, new_country_budget)
			sort.Slice(top_five, func(i, j int) bool {
				return top_five[i].Budget > top_five[j].Budget
			})
		} else {
			if top_five[4].Budget < budget {
				top_five[4] = new_country_budget
				sort.Slice(top_five, func(i, j int) bool {
					return top_five[i].Budget > top_five[j].Budget
				})
			}
		}

	}

	return top_five
}

func NewTopFiveCountryBudget(config TopFiveCountryBudgetConfig, storage_base_dir string) *TopFiveCountryBudget {
	log.Infof("TopFiveCountryBudget: %+v", config)
	grouped_elements, _ := common_statefull_worker.GetElements[[]CountrByBudget](storage_base_dir)

	my_id, _ := common_statefull_worker.GetMyId(storage_base_dir)
	eof_id := make(map[string]string)
	node_id := make(map[string]string)
	for key, val := range my_id {
		if len(val) == 2 {
			eof_id[key] = val[0]
			node_id[key] = val[1]
		}
	}

	worker, err := worker.NewWorker(config.WorkerConfig, 1)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &TopFiveCountryBudget{
		Worker:           *worker,
		top_five:         grouped_elements,
		storage_base_dir: storage_base_dir,
		eof_id:           eof_id,
		node_id:          node_id,
	}
}
