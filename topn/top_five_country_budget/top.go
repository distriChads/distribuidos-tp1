package top_five_country_budget

import (
	worker "distribuidos-tp1/common/worker/worker"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type TopFiveCountryBudgetConfig struct {
	worker.WorkerConfig
}

type TopFiveCountryBudget struct {
	worker.Worker
}

var log = logging.MustGetLogger("top_five_country_budget")

type TopFiveCountrByBudget struct {
	Country string
	Budget  int
}

// ---------------------------------
// MESSAGE FORMAT: COUNTRY|BUDGET
// ---------------------------------
const COUNTRY = 0
const BUDGET = 1

func updateTopFive(lines []string, top_five []TopFiveCountrByBudget) []TopFiveCountrByBudget {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		country := parts[COUNTRY]
		budget, err := strconv.Atoi(parts[BUDGET])
		if err != nil {
			continue
		}
		if len(top_five) < 5 {
			top_five = append(top_five, TopFiveCountrByBudget{Country: country, Budget: budget})
			sort.Slice(top_five, func(i, j int) bool {
				return top_five[i].Budget > top_five[j].Budget
			})
		} else {
			if top_five[4].Budget < budget {
				top_five[4] = TopFiveCountrByBudget{Country: country, Budget: budget}
				sort.Slice(top_five, func(i, j int) bool {
					return top_five[i].Budget > top_five[j].Budget
				})
			}
		}

	}
	return top_five
}

func mapToLines(top_five []TopFiveCountrByBudget) string {
	var lines []string
	for _, country_in_top := range top_five {
		line := fmt.Sprintf("%s%s%d", country_in_top.Country, worker.MESSAGE_SEPARATOR, country_in_top.Budget)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func NewTopFiveCountryBudget(config TopFiveCountryBudgetConfig) *TopFiveCountryBudget {
	log.Infof("TopFiveCountryBudget: %+v", config)
	return &TopFiveCountryBudget{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
	}
}

func (f *TopFiveCountryBudget) RunWorker() error {
	log.Info("Starting TopFiveCountryBudget worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	var top_five []TopFiveCountrByBudget
	for message := range msgs {
		message_str := string(message.Body)
		if message_str == worker.MESSAGE_EOF {
			break
		}
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		top_five = updateTopFive(lines, top_five)
		// message.Ack(false)
	}
	message_to_send := mapToLines(top_five)
	log.Infof("Top 5 countries by budget: %s", message_to_send)
	send_queue_key := f.Worker.OutputExchange.RoutingKeys[0] // los topN son nodos unicos, y solo le envian al server
	err = worker.SendMessage(f.Worker, message_to_send, send_queue_key)
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	return nil
}
