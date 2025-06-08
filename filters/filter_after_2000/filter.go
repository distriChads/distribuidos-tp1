package filterafter2000

import (
	worker "distribuidos-tp1/common/worker/worker"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_after_2000")

type FilterByAfterYear2000Config struct {
	worker.WorkerConfig
}

type FilterByAfterYear2000 struct {
	worker.Worker
	// eofs map[string]int
}

func NewFilterByAfterYear2000(config FilterByAfterYear2000Config) *FilterByAfterYear2000 {
	log.Infof("FilterByAfterYear2000: %+v", config)
	return &FilterByAfterYear2000{
		Worker: worker.Worker{
			Exchange:      config.Exchange,
			MessageBroker: config.MessageBroker,
		},
		// eofs: make(map[string]int),
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|...
// ---------------------------------
const ID = 0
const TITLE = 1
const DATE = 2

func (f *FilterByAfterYear2000) Filter(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		raw_year := strings.Split(parts[DATE], "-")[0]
		year, err := strconv.Atoi(raw_year)
		if err != nil {
			continue
		}
		if year >= 2000 {
			result = append(result, strings.TrimSpace(parts[ID])+worker.MESSAGE_SEPARATOR+strings.TrimSpace(parts[TITLE]))
		}
	}
	return result
}

func (f *FilterByAfterYear2000) HandleEOF(client_id string) error {
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

func (f *FilterByAfterYear2000) SendMessage(message_to_send []string, client_id string) error {
	for _, line := range message_to_send {
		// TODO: clean up commented code, the hasher node is used to send messages
		// parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		if len(message_to_send) != 0 {
			// id, err := strconv.Atoi(parts[ID])
			// if err != nil {
			// 	return err
			// }
			send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
			message := client_id + worker.MESSAGE_SEPARATOR + line
			err := worker.SendMessage(f.Worker, message, send_queue_key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
