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
	Worker *worker.Worker
}

func NewFilterByAfterYear2000(config FilterByAfterYear2000Config) *FilterByAfterYear2000 {
	log.Infof("FilterByAfterYear2000: %+v", config)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &FilterByAfterYear2000{
		Worker: worker,
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

func (f *FilterByAfterYear2000) HandleEOF(client_id string, message_id string) error {
	f.SendMessage([]string{worker.MESSAGE_EOF}, client_id, message_id)
	return nil
}

func (f *FilterByAfterYear2000) SendMessage(message_to_send []string, client_id string, message_id string) error {
	for _, line := range message_to_send {
		// TODO: clean up commented code, the hasher node is used to send messages
		// parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		if len(message_to_send) != 0 {
			// id, err := strconv.Atoi(parts[ID])
			// if err != nil {
			// 	return err
			// }
			send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
			message := client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + line
			err := f.Worker.SendMessage(message, send_queue_key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FilterByAfterYear2000) CloseWorker() {
	if f.Worker != nil {
		f.Worker.CloseWorker()
	}
	log.Info("FilterByAfterYear2000 worker closed")
}
