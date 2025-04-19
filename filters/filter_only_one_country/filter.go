package filter_only_one_country

import (
	worker "distribuidos-tp1/common/worker/worker"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_only_one_country")

type FilterByOnlyOneCountryConfig struct {
	worker.WorkerConfig
}

type FilterByOnlyOneCountry struct {
	worker.Worker
}

func filterByOnlyOneCountry(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, "|")
		countries := strings.Split(parts[3], ",")
		if len(countries) == 1 {
			result = append(result, strings.TrimSpace(line))
		}
	}
	return result
}

func NewFilterByOnlyOneCountry(config FilterByOnlyOneCountryConfig) *FilterByOnlyOneCountry {
	log.Infof("NewFilterByOnlyOneCountry: %+v", config)
	return &FilterByOnlyOneCountry{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
	}
}

func (f *FilterByOnlyOneCountry) RunWorker() error {
	log.Info("Starting FilterByOnlyOneCountry worker")
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
		result := filterByOnlyOneCountry(lines)
		err := worker.SendMessage(f.Worker, []byte(strings.Join(result, "\n")))
		if err != nil {
			log.Infof("Error sending message: %s", err.Error())
		}
	}

	return nil
}

func (f *FilterByOnlyOneCountry) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
