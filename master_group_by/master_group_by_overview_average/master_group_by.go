package master_group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type MasterGroupByOverviewAndAvgConfig struct {
	worker.WorkerConfig
}

var log = logging.MustGetLogger("master_group_by_movie_average")

type MasterGroupByOverviewAndAvg struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
}

// ---------------------------------
// MESSAGE FORMAT: OVERVIEW|AVERAGE
// ---------------------------------
const OVERVIEW = 0
const AVERAGE = 1

func groupByOverviewAndUpdate(lines []string, grouped_elements map[string]float64) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		average, err := strconv.ParseFloat(parts[AVERAGE], 64)
		if err != nil {
			continue
		}

		grouped_elements[parts[OVERVIEW]] += average

	}
}

func storeGroupedElements(results map[string]float64) {
	// TODO: Dumpear el hashmap a un archivo
}

func mapToLines(grouped_elements map[string]float64) string {
	var lines []string
	for movie, average := range grouped_elements {
		line := fmt.Sprintf("%s%s%f", movie, worker.MESSAGE_SEPARATOR, average)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func getGroupedElements() map[string]float64 {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByOverviewAndAvg(config MasterGroupByOverviewAndAvgConfig, messages_before_commit int, expected_eof int) *MasterGroupByOverviewAndAvg {
	log.Infof("MasterGroupByOverviewAndAvg: %+v", config)
	return &MasterGroupByOverviewAndAvg{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		expected_eof:           expected_eof,
	}
}

func (f *MasterGroupByOverviewAndAvg) RunWorker() error {
	log.Info("Starting MasterGroupByOverviewAndAvg worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	messages_before_commit := 0
	grouped_elements := make(map[string]float64)
	eof_counter := 0
	for message := range msgs {
		message_str := string(message.Body)
		if message_str == worker.MESSAGE_EOF {
			eof_counter++
			if eof_counter == f.expected_eof {
				break
			}
			continue
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		groupByOverviewAndUpdate(lines, grouped_elements)
		if messages_before_commit >= f.messages_before_commit {
			storeGroupedElements(grouped_elements)
			messages_before_commit = 0
		}
		// message.Ack(false)
	}
	message_to_send := mapToLines(grouped_elements)
	send_queue_key := f.Worker.OutputExchange.RoutingKeys[0] // POR QUE VA A ENVIAR A UN UNICO NODO MAESTRO
	log.Infof("Sending result: %s", message_to_send)
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
