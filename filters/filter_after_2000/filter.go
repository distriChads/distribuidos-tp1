package filterafter2000

import (
	worker "distribuidos-tp1/common/worker/worker"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_after_2000")

type FilterByAfterYear2000Config struct {
	worker.WorkerConfig
}

type FilterByAfterYear2000 struct {
	worker.Worker
}

func filterByYearAfter2000(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, "|")
		raw_year := strings.Split(parts[2], "-")[0]
		year, err := strconv.Atoi(raw_year)
		if err != nil {
			continue
		}
		if year >= 2000 {
			result = append(result, strings.TrimSpace(line))
		}
	}
	return result
}

func NewFilterByAfterYear2000(config FilterByAfterYear2000Config) *FilterByAfterYear2000 {
	log.Infof("FilterByAfterYear2000: %+v", config)
	return &FilterByAfterYear2000{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
	}
}

func (f *FilterByAfterYear2000) RunWorker() error {
	log.Info("Starting FilterByAfterYear2000 worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}

	for message := range msgs {
		message := string(message.Body)
		if message == "EOF" {
			err := worker.SendMessage(f.Worker, []byte("EOF"))
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
			break
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		result := filterByYearAfter2000(lines)
		var message_buffer []string
		for _, r := range result {
			parts := strings.Split(r, "|")
			title_and_id := strings.TrimSpace(parts[0]) + "|" + strings.TrimSpace(parts[1])
			message_buffer = append(message_buffer, title_and_id)
		}
		message_to_send := strings.Join(message_buffer, "\n")
		if len(message_to_send) != 0 {
			err := worker.SendMessage(f.Worker, []byte(message_to_send))
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
		}
	}

	return nil
}

func (f *FilterByAfterYear2000) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
