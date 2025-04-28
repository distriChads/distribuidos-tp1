package common_filter

import (
	worker "distribuidos-tp1/common/worker/worker"

	"strings"

	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type Filter interface {
	Filter(lines []string) []string
	HandleEOF() error
	SendMessage(lines []string) error
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
		log.Debugf("Received message: %s", message_str)
		if message_str == worker.MESSAGE_EOF {
			err := f.HandleEOF()
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
				return err
			}
			message.Ack(false)
			continue
		}
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		filtered_lines := f.Filter(lines)
		err := f.SendMessage(filtered_lines)
		if err != nil {
			log.Infof("Error sending message: %s", err.Error())
			return err
		}
		message.Ack(false)
	}

	return nil
}
