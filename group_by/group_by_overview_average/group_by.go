package group_by_overview_average

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

var log = logging.MustGetLogger("group_by_overview_average")

type GroupByOverviewAndAvg struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]RevenueBudgetCount
	eofs                   map[string]int
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

func storeGroupedElements(results map[string]RevenueBudgetCount, client_id string) {
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
		expected_eof:           eof_counter,
		eofs:                   make(map[string]int),
		grouped_elements:       make(map[string]map[string]RevenueBudgetCount),
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

	for message := range msgs {
		message_str := string(message.Body)
		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[0]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[1]
		if _, ok := f.grouped_elements[client_id]; !ok {
			f.grouped_elements[client_id] = make(map[string]RevenueBudgetCount)
		}
		if _, ok := f.eofs[client_id]; !ok {
			f.eofs[client_id] = 0
		}
		if message_str == worker.MESSAGE_EOF {
			f.eofs[client_id]++
			if f.eofs[client_id] >= f.expected_eof {
				sendResult(f, client_id)
				delete(f.grouped_elements, client_id)
				delete(f.eofs, client_id)
			}
			message.Ack(false)
			continue
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		groupByOverviewAndUpdate(lines, f.grouped_elements[client_id])
		if messages_before_commit >= f.messages_before_commit {
			storeGroupedElements(f.grouped_elements[client_id], client_id)
			messages_before_commit = 0
		}
		message.Ack(false)
	}

	return nil
}

func sendResult(f *GroupByOverviewAndAvg, client_id string) error {
	message_to_send := mapToLines(f.grouped_elements[client_id])
	send_queue_key := f.Worker.OutputExchange.RoutingKeys[0] // POR QUE VA A ENVIAR A UN UNICO NODO MAESTRO
	message_to_send = client_id + worker.MESSAGE_SEPARATOR + message_to_send
	err := worker.SendMessage(f.Worker, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	message_to_send = client_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
	err = worker.SendMessage(f.Worker, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	return nil
}
