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

func NewFilterByArgentina(config FilterByArgentinaConfig) *FilterByArgentina {
	log.Infof("NewFilterByArgentina: %+v", config)
	return &FilterByArgentina{
		Worker: worker.Worker{
			Exchange:      config.Exchange,
			MessageBroker: config.MessageBroker,
		},
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|...
// ---------------------------------
const COUNTRIES = 3

func (f *FilterByArgentina) Filter(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		countries := strings.Split(parts[COUNTRIES], worker.MESSAGE_ARRAY_SEPARATOR)
		for _, country := range countries {
			if strings.TrimSpace(country) == "AR" {
				result = append(result, strings.TrimSpace(line))
				break
			}
		}
	}
	return result
}

func (f *FilterByArgentina) HandleEOF(client_id string) error {
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

func (f *FilterByArgentina) SendMessage(message_to_send []string, client_id string) error {
	message := strings.Join(message_to_send, "\n")
	if len(message) != 0 {
		send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
		// send_queue_key := f.Worker.OutputExchange.RoutingKeys[f.queue_to_send]
		message = client_id + worker.MESSAGE_SEPARATOR + message
		err := worker.SendMessage(message, send_queue_key)
		// f.queue_to_send = f.Worker.Exchange.OutputRoutingKeys[0]
		if err != nil {
			return err
		}
		log.Debugf("Sent message to output exchange: %s", message)
	}
	return nil
}
