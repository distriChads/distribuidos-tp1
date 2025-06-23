package filter_only_one_country

import (
	buffer "distribuidos-tp1/common/worker/hasher"
	worker "distribuidos-tp1/common/worker/worker"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_only_one_country")

type FilterByOnlyOneCountryConfig struct {
	worker.WorkerConfig
}

type FilterByOnlyOneCountry struct {
	Worker *worker.Worker
	buffer *buffer.HasherContainer
}

func NewFilterByOnlyOneCountry(config FilterByOnlyOneCountryConfig) *FilterByOnlyOneCountry {
	log.Infof("FilterByOnlyOneCountry: %+v", config)
	worker, err := worker.NewWorker(config.WorkerConfig, 1)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	dict := make(map[string]int)
	for nodeType, routingKeys := range config.WorkerConfig.Exchange.OutputRoutingKeys {
		dict[nodeType] = len(routingKeys)
	}
	buffer := buffer.NewHasherContainer(dict)

	return &FilterByOnlyOneCountry{
		Worker: worker,
		buffer: buffer,
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|...
// ---------------------------------
const COUNTRIES = 3

func (f *FilterByOnlyOneCountry) Filter(lines []string) bool {
	anyMoviesFound := false
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_id, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		countries := strings.Split(parts[COUNTRIES], worker.MESSAGE_ARRAY_SEPARATOR)
		if len(countries) == 1 {
			f.buffer.AddMessage(movie_id, strings.TrimSpace(line))
			anyMoviesFound = true
		}
	}
	return anyMoviesFound
}

func (f *FilterByOnlyOneCountry) HandleEOF(client_id string, message_id string) error {
	for _, output_routing_keys := range f.Worker.Exchange.OutputRoutingKeys {
		for _, output_key := range output_routing_keys {
			message := client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF + "\n"
			err := f.Worker.SendMessage(message, output_key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FilterByOnlyOneCountry) SendMessage(client_id string, message_id string) error {
	for node_type := range f.Worker.Exchange.OutputRoutingKeys {
		messages_to_send := f.buffer.GetMessages(node_type)
		for routing_key_index, message := range messages_to_send {
			if len(message) != 0 {
				send_queue_key := f.Worker.Exchange.OutputRoutingKeys[node_type][routing_key_index]
				message = client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + message + "\n"
				err := f.Worker.SendMessage(message, send_queue_key)
				if err != nil {
					return err
				}
				log.Debugf("Sent message to output exchange: %s", message)
			}
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
