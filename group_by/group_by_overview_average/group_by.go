package group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type GroupByOverviewAndAvgConfig struct {
	worker.WorkerConfig
}

var log = logging.MustGetLogger("group_by_movie_average")

type GroupByOverviewAndAvg struct {
	worker.Worker
	messages_before_commit int
	eof_counter            int
}

type RevenueBudgetCount struct {
	count   int
	revenue float64
	budget  float64
}

// ---------------------------------
// MESSAGE FORMAT: OVERVIEW|BUDGET|REVENUE
// ---------------------------------
const OVERVIEW = 0
const BUDGET = 1
const REVENUE = 2

func groupByOverviewAndUpdate(lines []string, grouped_elements map[string]RevenueBudgetCount) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		budget, err := strconv.ParseFloat(parts[BUDGET], 64)
		if err != nil {
			continue
		}
		revenue, err := strconv.ParseFloat(parts[REVENUE], 64)
		if err != nil {
			continue
		}

		current := grouped_elements[parts[OVERVIEW]]
		current.budget += budget
		current.revenue += revenue
		current.count += 1
		grouped_elements[parts[OVERVIEW]] = current

	}
}

func storeGroupedElements(results map[string]RevenueBudgetCount) {
	// TODO: Dumpear el hashmap a un archivo
}

func mapToLines(grouped_elements map[string]RevenueBudgetCount) string {
	var lines []string
	for overview, value := range grouped_elements {
		result := 0.0
		if value.budget > 0 {
			result = value.revenue / value.budget
		}
		average := result / float64(value.count)
		line := fmt.Sprintf("%s%s%f", overview, worker.MESSAGE_SEPARATOR, average)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByOverviewAndAvg(config GroupByOverviewAndAvgConfig, messages_before_commit int, eof_counter int) *GroupByOverviewAndAvg {
	log.Infof("GroupByOverviewAndAvg: %+v", config)
	return &GroupByOverviewAndAvg{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		eof_counter:            eof_counter,
	}
}

func (f *GroupByOverviewAndAvg) RunWorker() error {
	log.Info("Starting GroupByOverviewAndAvg worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	messages_before_commit := 0
	grouped_elements := make(map[string]RevenueBudgetCount)
	i := 0
	for message := range msgs {
		message := string(message.Body)
		i++
		log.Infof("Received batch Number %d with message: %s", i, message)
		if message == worker.MESSAGE_EOF {
			f.eof_counter--
			if f.eof_counter <= 0 {
				break
			}
			continue
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message), "\n")
		groupByOverviewAndUpdate(lines, grouped_elements)
		if messages_before_commit >= f.messages_before_commit {
			storeGroupedElements(grouped_elements)
			messages_before_commit = 0
		}
	}
	message_to_send := mapToLines(grouped_elements)
	log.Info("Finished GroupByOverviewAndAvg worker with message: ", message_to_send)
	send_queue_key := f.Worker.OutputExchange.RoutingKeys[0] // POR QUE VA A ENVIAR A UN UNICO NODO MAESTRO
	err = worker.SendMessage(f.Worker, message_to_send, send_queue_key)
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	err = worker.SendMessage(f.Worker, worker.MESSAGE_EOF, send_queue_key)
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	return nil
}
