package top_five_country_budget

import (
	worker "distribuidos-tp1/common/worker"
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

var log = logging.MustGetLogger("filter_by_year")

type TopFiveCountrByBudget struct {
	Country string
	Budget  int
}

func updateTopFive(lines []string, top_five []TopFiveCountrByBudget) []TopFiveCountrByBudget {
	for _, line := range lines {
		parts := strings.Split(line, ",")
		country := parts[0]
		budget, err := strconv.Atoi(parts[1])
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

func mapToLines(top_five []TopFiveCountrByBudget) []string {
	var lines []string
	for _, country_in_top := range top_five {
		line := fmt.Sprintf("%s,%d", country_in_top.Country, country_in_top.Budget)
		lines = append(lines, line)
	}
	return lines
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
		log.Infof("Received message in top five: %s", string(message.Body))
		message := string(message.Body)
		if message == "EOF" {
			break
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		top_five = updateTopFive(lines, top_five)
	}
	message_to_send := mapToLines(top_five)
	err = worker.SendMessage(f.Worker, []byte(strings.Join(message_to_send, "\n")))
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	return nil
}

func (f *TopFiveCountryBudget) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
