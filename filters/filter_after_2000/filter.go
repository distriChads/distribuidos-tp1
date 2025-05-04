package filterafter2000

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/filters/common_filter"
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
	expected_eof int
	eofs         map[string]int
}

func NewFilterByAfterYear2000(config FilterByAfterYear2000Config, eof_counter int) *FilterByAfterYear2000 {
	log.Infof("FilterByAfterYear2000: %+v", config)
	return &FilterByAfterYear2000{
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
// MESSAGE FORMAT: ID|TITLE|DATE|...
// ---------------------------------
const ID = 0
const TITLE = 1
const DATE = 2

func (f *FilterByAfterYear2000) Filter(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		if len(parts) < 3 {
			continue
		}
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
	if _, ok := f.eofs[client_id]; !ok {
		f.eofs[client_id] = 0
	}
	f.eofs[client_id]++
	if f.eofs[client_id] >= f.expected_eof {
		log.Infof("Sending EOF for client %s", client_id)
		for _, queue_name := range f.Worker.OutputExchange.RoutingKeys {
			routing_key := queue_name
			message := client_id + "/" + worker.MESSAGE_EOF
			err := worker.SendMessage(f.Worker, message, routing_key)
			if err != nil {
				return err
			}
		}
		log.Infof("Client %s finished", client_id)
	}
	return nil
}

func (f *FilterByAfterYear2000) SendMessage(message_to_send []string, client_id string) error {
	for _, line := range message_to_send {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		if len(message_to_send) != 0 {
			id, err := strconv.Atoi(parts[ID])
			if err != nil {
				return err
			}
			line = client_id + "/" + line
			send_queue_key := f.Worker.OutputExchange.RoutingKeys[id%len(f.Worker.OutputExchange.RoutingKeys)]
			err = worker.SendMessage(f.Worker, line, send_queue_key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FilterByAfterYear2000) RunWorker(starting_message string) error {
	msgs, err := common_filter.Init(&f.Worker, starting_message)
	if err != nil {
		return err
	}
	return common_filter.RunWorker(f, msgs)
}
