package common_filter

import (
	worker "distribuidos-tp1/common/worker/worker"

	"strings"

	"github.com/op/go-logging"
)

type Filter interface {
	Filter(lines []string) []string
	HandleEOF(client_id string) error
	SendMessage(lines []string, client_id string) error
}

var log = logging.MustGetLogger("common_filter")

func Init(w *worker.Worker, starting_message string) error {
	log.Info(starting_message)

	err := worker.InitSender(w)
	if err != nil {
		return err
	}
	err = worker.InitReceiver(w)
	if err != nil {
		return err
	}

	return nil
}

func RunWorker(f Filter, w worker.Worker, starting_message string) error {
	err := Init(&w, starting_message)
	if err != nil {
		return err
	}

	for {
		message, _, err := worker.ReceivedMessages(w)
		if err != nil {
			log.Errorf("Fatal error in run worker")
			return err
		}
		message_str := string(message.Body)
		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[0]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[1]
		log.Debugf("Received message: %s", message_str)
		if len(message_str) == 0 {
			log.Warning("Received empty message")
			message.Ack(false)
			continue
		}
		if strings.TrimSpace(message_str) == worker.MESSAGE_EOF {
			err := f.HandleEOF(client_id)
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
				return err
			}
			message.Ack(false)
			continue
		}

		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		filtered_lines := f.Filter(lines)
		err = f.SendMessage(filtered_lines, client_id)
		if err != nil {
			log.Infof("Error sending message: %s", err.Error())
			return err
		}
		message.Ack(false)
	}
}
