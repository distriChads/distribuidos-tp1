package master_group_by_actor_count

import (
	worker "distribuidos-tp1/common/worker/worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type MasterGroupByActorAndCountConfig struct {
	worker.WorkerConfig
}

type MasterGroupByActorAndCount struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]int
	eofs                   map[string]int
}

var log = logging.MustGetLogger("master_group_by_actor_count")

// ---------------------------------
// MESSAGE FORMAT: ACTOR|COUNT
// ---------------------------------
const ACTOR = 0
const COUNT = 1

func groupByActorAndUpdate(lines []string, grouped_elements map[string]int) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		count, err := strconv.Atoi(parts[COUNT])
		if err != nil {
			continue
		}
		grouped_elements[parts[ACTOR]] += count
	}
}

func storeGroupedElements(results map[string]int, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func mapToLines(grouped_elements map[string]int) string {
	var lines []string
	for actor, count := range grouped_elements {
		line := fmt.Sprintf("%s%s%d", actor, worker.MESSAGE_SEPARATOR, count)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByActorAndCount(config MasterGroupByActorAndCountConfig, messages_before_commit int, expected_eof int) *MasterGroupByActorAndCount {
	log.Infof("MasterGroupByActorAndCount: %+v", config)
	return &MasterGroupByActorAndCount{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		expected_eof:           expected_eof,
		grouped_elements:       make(map[string]map[string]int),
		eofs:                   make(map[string]int),
	}
}

func (f *MasterGroupByActorAndCount) RunWorker() error {
	log.Info("Starting MasterGroupByActorAndCount worker")
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
			f.grouped_elements[client_id] = make(map[string]int)
		}
		if _, ok := f.eofs[client_id]; !ok {
			f.eofs[client_id] = 0
		}
		log.Infof("Message received: %s", message_str)
		if message_str == worker.MESSAGE_EOF {
			f.eofs[client_id]++
			if f.eofs[client_id] >= f.expected_eof {
				log.Infof("Sending result for client %s", client_id)
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
		groupByActorAndUpdate(lines, f.grouped_elements[client_id])
		if messages_before_commit >= f.messages_before_commit {
			storeGroupedElements(f.grouped_elements[client_id], client_id)
			messages_before_commit = 0
		}
		message.Ack(false)
	}

	return nil
}

func sendResult(f *MasterGroupByActorAndCount, client_id string) error {
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
