package filter_spain_2000

import (
	worker "distribuidos-tp1/common/worker/worker"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_after_2000")

type FilterBySpainAndOf2000Config struct {
	worker.WorkerConfig
}

type FilterBySpainAndOf2000 struct {
	worker.Worker
}

func NewFilterBySpainAndOf2000(config FilterBySpainAndOf2000Config) *FilterBySpainAndOf2000 {
	log.Infof("FilterBySpainAndOf2000: %+v", config)
	return &FilterBySpainAndOf2000{
		Worker: worker.Worker{
			Exchange:      config.Exchange,
			MessageBroker: config.MessageBroker,
		},
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|GENRES|...
// ---------------------------------
const TITLE = 1
const DATE = 2
const COUNTRIES = 3
const GENRES = 4

func (f *FilterBySpainAndOf2000) Filter(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		raw_year := strings.Split(parts[DATE], "-")[0]
		year, err := strconv.Atoi(raw_year)
		if err != nil {
			continue
		}
		if !(year >= 2000 && year < 2010) {
			continue
		}
		countries := strings.Split(parts[COUNTRIES], worker.MESSAGE_ARRAY_SEPARATOR)
		for _, country := range countries {
			if strings.TrimSpace(country) == "ES" {
				result = append(result, strings.TrimSpace(parts[TITLE])+worker.MESSAGE_SEPARATOR+strings.TrimSpace(parts[GENRES]))
				break
			}
		}

	}
	return result
}

func (f *FilterBySpainAndOf2000) HandleEOF(client_id string) error {
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

func (f *FilterBySpainAndOf2000) SendMessage(message_to_send []string, client_id string) error {
	message := strings.Join(message_to_send, "\n")
	if len(message) != 0 {
		// send_queue_key := f.Worker.OutputExchange.RoutingKeys[f.queue_to_send]
		send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
		message = client_id + worker.MESSAGE_SEPARATOR + message
		err := worker.SendMessage(f.Worker, message, send_queue_key)
		// f.queue_to_send = (f.queue_to_send + 1) % len(f.Worker.OutputExchange.RoutingKeys)
		if err != nil {
			return err
		}
	}
	return nil
}
