package filters

import (
	worker "distribuidos-tp1/common/worker"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_by_year")

type FilterByYearConfig struct {
	worker.WorkerConfig
	Year int
}

type FilterByYear struct {
	worker.Worker
	year int
}

func NewFilterByYear(config FilterByYearConfig) *FilterByYear {
	log.Infof("NewFilterByYear: %+v", config)
	return &FilterByYear{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		year: config.Year,
	}
}

func (f *FilterByYear) RunWorker() error {
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
		err := worker.SendMessage(f.Worker, message.Body)
		if err != nil {
			log.Infof("Error sending message: %s", err.Error())
		}
	}

	return nil
}

func (f *FilterByYear) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
