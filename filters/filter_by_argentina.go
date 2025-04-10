package filters

import (
	worker "distribuidos-tp1/common/worker"
	"strings"
)

type FilterByArgentinaConfig struct {
	worker.WorkerConfig
}

type FilterByArgentina struct {
	worker.Worker
}

func FilterByCountryArgentina(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, ",")
		countries := strings.Split(parts[0], "|")
		for _, country := range countries {
			if strings.TrimSpace(country) == "ARG" {
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
		result := FilterByCountryArgentina(lines)
		err := worker.SendMessage(f.Worker, []byte(strings.Join(result, "\n")))
		if err != nil {
			log.Infof("Error sending message: %s", err.Error())
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
