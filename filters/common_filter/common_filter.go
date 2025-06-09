package common_filter

import (
	"context"
	"distribuidos-tp1/common/worker/worker"
	worker_package "distribuidos-tp1/common/worker/worker"

	"strings"

	"github.com/op/go-logging"
)

type Filter interface {
	Filter(lines []string) []string
	HandleEOF(client_id string) error
	SendMessage(lines []string, client_id string) error
}

var log = logging.MustGetLogger("common_filter")

func RunWorker(f Filter, worker worker.Worker, ctx context.Context, starting_message string) error {
	log.Info(starting_message)

	for {
		message, _, err := worker.ReceivedMessages(ctx)
		if err != nil {
			log.Errorf("Fatal error in run worker")
			return err
		}
		message_str := string(message.Body)
		log.Debugf("Received message: %s", message_str)
		if len(message_str) == 0 {
			log.Warning("Received empty message")
			message.Ack(false)
			continue
		}
		client_id := strings.SplitN(message_str, worker_package.MESSAGE_SEPARATOR, 2)[0]
		message_str = strings.SplitN(message_str, worker_package.MESSAGE_SEPARATOR, 2)[1]
		if len(message_str) == 0 {
			log.Warning("Received empty message")
			message.Ack(false)
			continue
		}
		if strings.TrimSpace(message_str) == worker_package.MESSAGE_EOF {
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
