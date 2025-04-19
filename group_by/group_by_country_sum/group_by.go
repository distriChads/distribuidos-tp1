package group_by_country_sum

import (
	worker "distribuidos-tp1/common/worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type GroupByCountryAndSumConfig struct {
	worker.WorkerConfig
}

type GroupByCountryAndSum struct {
	worker.Worker
	messages_before_commit int
}

var log = logging.MustGetLogger("filter_by_year")

func groupByCountryAndSum(lines []string, grouped_elements map[string]int) {
	for _, line := range lines {
		parts := strings.Split(line, ",")
		budget, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		grouped_elements[parts[0]] += budget
	}
}

func storeGroupedElements(results map[string]int) {
	// TODO: Dumpear el hashmap a un archivo
}

func mapToLines(grouped_elements map[string]int) []string {
	var lines []string
	for country, budget := range grouped_elements {
		line := fmt.Sprintf("%s,%d", country, budget)
		lines = append(lines, line)
	}
	return lines
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByCountryAndSum(config GroupByCountryAndSumConfig, messages_before_commit int) *GroupByCountryAndSum {
	log.Infof("GroupByCountryAndSum: %+v", config)
	return &GroupByCountryAndSum{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
	}
}

func (f *GroupByCountryAndSum) RunWorker() error {
	log.Info("Starting GroupByCountryAndSum worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	messages_before_commit := 0
	grouped_elements := make(map[string]int)
	for message := range msgs {
		log.Infof("Received message in group by country and sum: %s", string(message.Body))
		message := string(message.Body)
		if message == "EOF" {
			break
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message), "\n")
		groupByCountryAndSum(lines, grouped_elements)
		if messages_before_commit >= f.messages_before_commit {
			storeGroupedElements(grouped_elements)
			messages_before_commit = 0
		}
	}
	message_to_send := mapToLines(grouped_elements)
	err = worker.SendMessage(f.Worker, []byte(strings.Join(message_to_send, "\n")))
	// TODO: Enviar a una cola de un agrupador "maestro" que haga la ultima agrupacion y este se lo envie al proximo chavoncito
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	err = worker.SendMessage(f.Worker, []byte("EOF"))
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	return nil
}

func (f *GroupByCountryAndSum) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
