package filter_only_one_country

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/filters/common_filter"
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
	expected_eof  int
	eofs          map[string]int
}

func NewFilterByOnlyOneCountry(config FilterByOnlyOneCountryConfig, eof_counter int) *FilterByOnlyOneCountry {
	log.Infof("NewFilterByOnlyOneCountry: %+v", config)
	return &FilterByOnlyOneCountry{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		expected_eof: eof_counter,
		eofs:         make(map[string]int),
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
	if _, ok := f.eofs[client_id]; !ok {
		f.eofs[client_id] = 0
	}
	f.eofs[client_id]++
	if f.eofs[client_id] >= f.expected_eof {
		log.Infof("Sending EOF for client %s", client_id)
		for _, queue_name := range f.Worker.OutputExchange.RoutingKeys {
			routing_key := queue_name
			message := client_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
			err := worker.SendMessage(f.Worker, message, routing_key)
			if err != nil {
				return err
			}
		}
		log.Infof("Client %s finished", client_id)
	}
	return nil
}

func (f *FilterByOnlyOneCountry) SendMessage(message_to_send []string, client_id string) error {
	message := strings.Join(message_to_send, "\n")
	if len(message) != 0 {
		send_queue_key := f.Worker.OutputExchange.RoutingKeys[f.queue_to_send]
		message = client_id + worker.MESSAGE_SEPARATOR + message
		err := worker.SendMessage(f.Worker, message, send_queue_key)
		f.queue_to_send = (f.queue_to_send + 1) % len(f.Worker.OutputExchange.RoutingKeys)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *FilterByOnlyOneCountry) RunWorker(starting_message string) error {
	msgs, err := common_filter.Init(&f.Worker, starting_message)
	if err != nil {
		return err
	}
	return common_filter.RunWorker(f, msgs)
}
