package filter_argentina

import (
	"distribuidos-tp1/common/worker/worker"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_argentina")

type FilterByArgentinaConfig struct {
	worker.WorkerConfig
}

type FilterByArgentina struct {
	worker.Worker
}

func filterByArgentina(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, "|")
		countries := strings.Split(parts[3], ",")
		for _, country := range countries {
			if strings.TrimSpace(country) == "AR" {
				result = append(result, strings.TrimSpace(line))
				break
			}
		}
	}
	return result
}

func NewFilterByArgentina(config FilterByArgentinaConfig) *FilterByArgentina {
	log.Infof("NewFilterByYear: %+v", config)
	return &FilterByArgentina{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
	}
}

func (f *FilterByArgentina) RunWorker() error {
	log.Info("Starting FilterByYear worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}

	for message := range msgs {
		log.Infof("Received message: %s", string(message.Body))
		message := string(message.Body)
		lines := strings.Split(strings.TrimSpace(message), "\n")
		result := filterByArgentina(lines)
		message_to_send := strings.Join(result, "\n")
		if len(message_to_send) != 0 {
			err := worker.SendMessage(f.Worker, []byte(message_to_send))
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
		}

	}

	return nil
}

func (f *FilterByArgentina) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
