package master_group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type MasterGroupByMovieAndAvgConfig struct {
	worker.WorkerConfig
}

var log = logging.MustGetLogger("master_group_by_movie_average")

type MasterGroupByMovieAndAvg struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]ScoreAndCount
	eofs                   map[string]int
}

type ScoreAndCount struct {
	count int
	score float64
}

// ---------------------------------
// MESSAGE FORMAT: TITLE|AVERAGE
// ---------------------------------
const TITLE = 0
const SCORE = 1

func groupByMovieAndUpdate(lines []string, grouped_elements map[string]ScoreAndCount) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		if len(parts) < 2 {
			log.Errorf("Invalid message format: %s", line)
		}
		average, err := strconv.ParseFloat(parts[SCORE], 64)
		if err != nil {
			continue
		}
		current := grouped_elements[parts[TITLE]]
		current.score += average
		current.count += 1
		grouped_elements[parts[TITLE]] = current

	}
}

func storeGroupedElements(results map[string]ScoreAndCount, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func mapToLines(grouped_elements map[string]ScoreAndCount) string {
	var lines []string
	for title, value := range grouped_elements {
		average := value.score / float64(value.count)
		line := fmt.Sprintf("%s%s%f", title, worker.MESSAGE_SEPARATOR, average)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByMovieAndAvg(config MasterGroupByMovieAndAvgConfig, messages_before_commit int, expected_eof int) *MasterGroupByMovieAndAvg {
	log.Infof("MasterGroupByMovieAndAvg: %+v", config)
	return &MasterGroupByMovieAndAvg{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		expected_eof:           expected_eof,
		grouped_elements:       make(map[string]map[string]ScoreAndCount),
		eofs:                   make(map[string]int),
	}
}

func (f *MasterGroupByMovieAndAvg) RunWorker() error {
	log.Info("Starting MasterGroupByMovieAndAvg worker")
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
			f.grouped_elements[client_id] = make(map[string]ScoreAndCount)
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
				log.Infof("Client %s finished", client_id)
			}
			message.Ack(false)
			continue
		}
		if len(message_str) == 0 {
			continue
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		groupByMovieAndUpdate(lines, f.grouped_elements[client_id])
		if messages_before_commit >= f.messages_before_commit {
			storeGroupedElements(f.grouped_elements[client_id], client_id)
			messages_before_commit = 0
		}
		message.Ack(false)
	}

	return nil
}

func sendResult(f *MasterGroupByMovieAndAvg, client_id string) error {
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
