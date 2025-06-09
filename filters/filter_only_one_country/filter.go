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
	Worker *worker.Worker
}

func NewFilterByOnlyOneCountry(config FilterByOnlyOneCountryConfig) *FilterByOnlyOneCountry {
	log.Infof("FilterByOnlyOneCountry: %+v", config)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &FilterByOnlyOneCountry{
		Worker: worker,
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|...
// ---------------------------------
const COUNTRIES = 3

func (f *FilterByOnlyOneCountry) Filter(lines []string) []string {
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

func (f *FilterByOnlyOneCountry) HandleEOF(client_id string) error {
	// if _, ok := f.eofs[client_id]; !ok {
	// 	f.eofs[client_id] = 0
	// }
	// f.eofs[client_id]++
	// if f.eofs[client_id] >= f.expected_eof {
	// 	log.Infof("Sending EOF for client %s", client_id)
	// 	for _, queue_name := range f.Worker.OutputExchange.RoutingKeys {
	// 		routing_key := queue_name
	// 		message := client_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
	// 		err := worker.SendMessage(f.Worker, message, routing_key)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	log.Infof("Client %s finished", client_id)
	// }
	return nil
}

func (f *FilterByOnlyOneCountry) SendMessage(message_to_send []string, client_id string) error {
	message := strings.Join(message_to_send, "\n")
	if len(message) != 0 {
		// send_queue_key := f.Worker.OutputExchange.RoutingKeys[f.queue_to_send]
		send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
		message = client_id + worker.MESSAGE_SEPARATOR + message
		err := f.Worker.SendMessage(message, send_queue_key)
		// f.queue_to_send = (f.queue_to_send + 1) % len(f.Worker.OutputExchange.RoutingKeys)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *FilterByOnlyOneCountry) CloseWorker() {
	if f.Worker != nil {
		f.Worker.CloseWorker()
	}
	log.Info("FilterByOnlyOneCountry worker closed")
}
