package common_statefull_worker

import (
	worker "distribuidos-tp1/common/worker/worker"

	"strings"

	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type StatefullWorker interface {
	UpdateState(lines []string, client_id string)
	HandleEOF(client_id string) error
	MapToLines(client_id string) string
	ShouldCommit(messages_before_commit int, client_id string) bool
	NewClient(client_id string)
}

var log = logging.MustGetLogger("common_group_by")

func SendResult(w worker.Worker, s StatefullWorker, client_id string) error {
	send_queue_key := w.OutputExchange.RoutingKeys[0] // POR QUE VA A ENVIAR A UN UNICO NODO MAESTRO
	message_to_send := client_id + worker.MESSAGE_SEPARATOR + s.MapToLines(client_id)
	err := worker.SendMessage(w, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	message_to_send = client_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
	err = worker.SendMessage(w, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	return nil
}

func Init(w *worker.Worker, starting_message string) (<-chan amqp091.Delivery, error) {
	log.Info(starting_message)

	err := worker.InitSender(w)
	if err != nil {
		return nil, err
	}

	err = worker.InitReceiver(w)

	if err != nil {
		return nil, err
	}

	msgs, err := worker.ReceivedMessages(*w)
	if err != nil {
		return nil, err
	}

	return msgs, nil
}

func RunWorker(s StatefullWorker, msgs <-chan amqp091.Delivery) error {

	messages_before_commit := 0
	for message := range msgs {
		message_str := string(message.Body)
		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[0]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[1]
		s.NewClient(client_id)

		if message_str == worker.MESSAGE_EOF {
			err := s.HandleEOF(client_id)
			if err != nil {
				return err
			}
			message.Ack(false)
			continue
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		s.UpdateState(lines, client_id)
		if s.ShouldCommit(messages_before_commit, client_id) {
			messages_before_commit = 0
		}
		message.Ack(false)
	}

	return nil
}
