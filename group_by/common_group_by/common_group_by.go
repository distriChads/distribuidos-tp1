package common_group_by

import (
	worker "distribuidos-tp1/common/worker/worker"

	"strings"

	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type GroupBy interface {
	GroupByAndUpdate(lines []string, client_id string) // le paso el lines, el que la reeimplemente llama a esta y llama a su groupByAndUpdate correspondiente
	HandleEOF(client_id string) error
	MapToLines(client_id string) string // cuando llame a esto hago el g.grouped_elements[client_id]
	ShouldCommit(messages_before_commit int, client_id string) bool
	NewClient(client_id string)
}

var log = logging.MustGetLogger("common_filter")

func SendResult(w worker.Worker, g GroupBy, client_id string) error {
	message_to_send := client_id + "/" + g.MapToLines(client_id)
	send_queue_key := w.OutputExchange.RoutingKeys[0]
	err := worker.SendMessage(w, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	message_to_send = client_id + "/" + worker.MESSAGE_EOF
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

func RunWorker(g GroupBy, msgs <-chan amqp091.Delivery) error {

	messages_before_commit := 0
	for message := range msgs {
		message_str := string(message.Body)
		client_id := strings.Split(message_str, "/")[0]
		message_str = strings.Split(message_str, "/")[1]
		g.NewClient(client_id)

		if strings.TrimSpace(message_str) == worker.MESSAGE_EOF {
			err := g.HandleEOF(client_id)
			if err != nil {
				return err
			}
			message.Ack(false)
			continue
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		g.GroupByAndUpdate(lines, client_id)
		if g.ShouldCommit(messages_before_commit, client_id) {
			messages_before_commit = 0
		}
		message.Ack(false)
	}

	return nil
}
