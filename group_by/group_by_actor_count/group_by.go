package group_by_actor_count

import (
	worker "distribuidos-tp1/common/worker/worker"
	"fmt"
	"strings"

	"github.com/op/go-logging"
)

type GroupByActorAndCountConfig struct {
	worker.WorkerConfig
}

type GroupByActorAndCount struct {
	worker.Worker
	messages_before_commit int
}

var log = logging.MustGetLogger("group_by_actor_count")

func groupByActorAndUpdate(lines []string, grouped_elements map[string]int) {
	for _, line := range lines {
		actors := strings.Split(line, ",")
		for _, actor := range actors {
			grouped_elements[actor] += 1
		}
	}
}

func storeGroupedElements(results map[string]int) {
	// TODO: Dumpear el hashmap a un archivo
}

func mapToLines(grouped_elements map[string]int) []string {
	var lines []string
	for actor, value := range grouped_elements {
		line := fmt.Sprintf("%s,%d", actor, value)
		lines = append(lines, line)
	}
	return lines
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByActorAndCount(config GroupByActorAndCountConfig, messages_before_commit int) *GroupByActorAndCount {
	log.Infof("GroupByActorAndCount: %+v", config)
	return &GroupByActorAndCount{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
	}
}

func (f *GroupByActorAndCount) RunWorker() error {
	log.Info("Starting GroupByActorAndCount worker")
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
		log.Infof("Received message in group by actor: %s", string(message.Body))
		message := string(message.Body)
		if message == "EOF" {
			break
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message), "\n")
		groupByActorAndUpdate(lines, grouped_elements)
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

func (f *GroupByActorAndCount) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
