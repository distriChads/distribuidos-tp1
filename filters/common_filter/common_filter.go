package common_filter

import (
	worker "distribuidos-tp1/common/worker/worker"

	"strings"

	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type Filter interface {
	Filter(lines []string) []string
	HandleEOF(client_id string) error
	SendMessage(lines []string, client_id string) error
}

var log = logging.MustGetLogger("common_filter")

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

func RunWorker(f Filter, msgs <-chan amqp091.Delivery) error {

	for message := range msgs {
		message_str := string(message.Body)
		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[0]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[1]
		log.Debugf("Received message: %s", message_str)
		if strings.TrimSpace(message_str) == worker.MESSAGE_EOF {
			err := f.HandleEOF(client_id)
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
				return err
			}
			message.Ack(false)
			continue
		}
		if len(message_str) == 0 {
			continue
		}
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		filtered_lines := f.Filter(lines)
		err := f.SendMessage(filtered_lines, client_id)
		if err != nil {
			log.Infof("Error sending message: %s", err.Error())
			return err
		}
		message.Ack(false)
	}

	return nil
}
