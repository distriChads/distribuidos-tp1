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
	queue_to_send int
	eof_counter   int
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|...
// ---------------------------------
const COUNTRIES = 3

func filterByOnlyOneCountry(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		countries := strings.Split(parts[COUNTRIES], worker.MESSAGE_ARRAY_SEPARATOR)
		if len(countries) == 1 {
			result = append(result, strings.TrimSpace(line))
		}
	}
	return result
}

func NewFilterByOnlyOneCountry(config FilterByOnlyOneCountryConfig, eof_counter int) *FilterByOnlyOneCountry {
	log.Infof("NewFilterByOnlyOneCountry: %+v", config)
	return &FilterByOnlyOneCountry{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		eof_counter: eof_counter,
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
		message := string(message.Body)
		if message == worker.MESSAGE_EOF {
			f.eof_counter--
			if f.eof_counter <= 0 {

				for _, queue_name := range f.Worker.OutputExchange.RoutingKeys {
					err := worker.SendMessage(f.Worker, worker.MESSAGE_EOF, queue_name)
					if err != nil {
						log.Infof("Error sending message: %s", err.Error())
					}
				}
				break
			}
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		filtered_lines := filterByOnlyOneCountry(lines)
		message_to_send := strings.Join(filtered_lines, "\n")
		if len(message_to_send) != 0 {
			send_queue_key := f.Worker.OutputExchange.RoutingKeys[f.queue_to_send]
			err := worker.SendMessage(f.Worker, message_to_send, send_queue_key)
			f.queue_to_send = (f.queue_to_send + 1) % len(f.Worker.OutputExchange.RoutingKeys)
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
		}
	}

	return nil
}
