package common_filter

import (
	"context"
	"distribuidos-tp1/common/worker/worker"
	worker_package "distribuidos-tp1/common/worker/worker"

	"strings"

	"github.com/op/go-logging"
)

type Filter interface {
	Filter(lines []string)
	HandleEOF(client_id string, message_id string) error
	SendMessage(client_id string, message_id string) error
}

var log = logging.MustGetLogger("common_filter")

func RunWorker(f Filter, worker worker.Worker, ctx context.Context, starting_message string) error {
	log.Info(starting_message)

	for {
		msg, _, err := worker.ReceivedMessages(ctx)
		if err != nil {
			log.Errorf("Fatal error in run worker: %v", err)
			return err
		}

		message_str := string(msg.Body)
		log.Debugf("Received message: %s", message_str)
		if len(message_str) == 0 {
			log.Warning("Received empty message")
			msg.Ack(false)
			continue
		}

		client_id := strings.SplitN(message_str, worker_package.MESSAGE_SEPARATOR, 3)[0]
		message_id := strings.SplitN(message_str, worker_package.MESSAGE_SEPARATOR, 3)[1]
		message_str = strings.SplitN(message_str, worker_package.MESSAGE_SEPARATOR, 3)[2]

		if len(message_str) == 0 {
			log.Warning("Received empty message")
			msg.Ack(false)
			continue
		}

		if strings.TrimSpace(message_str) == worker_package.MESSAGE_EOF {
			err := f.HandleEOF(client_id, message_id)
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
				return err
			}
			msg.Ack(false)
			continue
		}

		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		f.Filter(lines)
		err = f.SendMessage(client_id, message_id)
		if err != nil {
			log.Infof("Error sending message: %s", err.Error())
			return err
		}
		log.Debug("Sent message")

		msg.Ack(false)
	}
}
