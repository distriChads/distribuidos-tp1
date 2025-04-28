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
	eof_counter int
}

func NewFilterByAfterYear2000(config FilterByAfterYear2000Config, eof_counter int) *FilterByAfterYear2000 {
	log.Infof("FilterByAfterYear2000: %+v", config)
	return &FilterByAfterYear2000{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		eof_counter: eof_counter,
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

func (f *FilterByAfterYear2000) HandleEOF() error {
	log.Info("Received EOF")
	f.eof_counter--
	if f.eof_counter <= 0 {
		log.Info("Sending EOF")
		for _, queue_name := range f.Worker.OutputExchange.RoutingKeys {
			err := worker.SendMessage(f.Worker, worker.MESSAGE_EOF, queue_name)
			if err != nil {
				return err
			}
		}
		log.Info("Finished sending EOF")

	}
	return nil
}

func (f *FilterByAfterYear2000) SendMessage(message_to_send []string) error {
	for _, line := range message_to_send {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		if len(message_to_send) != 0 {
			id, err := strconv.Atoi(parts[ID])
			if err != nil {
				return err
			}
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
