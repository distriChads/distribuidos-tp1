package filter_argentina

import (
	"distribuidos-tp1/common/worker/worker"
	"strconv"
	"strings"

	buffer "distribuidos-tp1/common/worker/hasher"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_argentina")

type FilterByArgentinaConfig struct {
	worker.WorkerConfig
}

type FilterByArgentina struct {
	Worker *worker.Worker
	buffer *buffer.HasherContainer
}

func NewFilterByArgentina(config FilterByArgentinaConfig) *FilterByArgentina {
	log.Infof("FilterByArgentina: %+v", config)
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

	return &FilterByArgentina{
		Worker: worker,
		buffer: buffer,
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|...
// ---------------------------------
const COUNTRIES = 3

func (f *FilterByArgentina) Filter(lines []string) bool {
	argMovie := false
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_id, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		countries := strings.SplitSeq(parts[COUNTRIES], worker.MESSAGE_ARRAY_SEPARATOR)
		for country := range countries {
			if strings.TrimSpace(country) == "AR" {
				f.buffer.AddMessage(movie_id, strings.TrimSpace(line))
				argMovie = true
				break
			}
		}
	}
	return argMovie
}

func (f *FilterByArgentina) HandleEOF(client_id string, message_id string) error {
	for _, output_routing_keys := range f.Worker.Exchange.OutputRoutingKeys {
		for _, output_key := range output_routing_keys {
			message := client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
			err := f.Worker.SendMessage(message, output_key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FilterByArgentina) SendMessage(client_id string, message_id string) error {
	for node_type := range f.Worker.Exchange.OutputRoutingKeys {
		messages_to_send := f.buffer.GetMessages(node_type)
		for routing_key_index, message := range messages_to_send {
			if len(message) != 0 {
				routing_key := f.Worker.Exchange.OutputRoutingKeys[node_type][routing_key_index]
				message = client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + message
				err := f.Worker.SendMessage(message, routing_key)
				if err != nil {
					return err
				}
				log.Debugf("Sent message to output exchange: %s", message)
			}
		}

	}
	return nil
}

func (f *FilterByArgentina) CloseWorker() {
	if f.Worker != nil {
		f.Worker.CloseWorker()
	}
	log.Info("FilterByArgentina worker closed")
}
