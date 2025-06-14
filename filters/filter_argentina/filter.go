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
	Worker *worker.Worker
}

func NewFilterByArgentina(config FilterByArgentinaConfig) *FilterByArgentina {
	log.Infof("FilterByArgentina: %+v", config)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &FilterByArgentina{
		Worker: worker,
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
	f.SendMessage([]string{worker.MESSAGE_EOF}, client_id)
	return nil
}

func (f *FilterByArgentina) SendMessage(message_to_send []string, client_id string) error {
	message := strings.Join(message_to_send, "\n")
	if len(message) != 0 {
		send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
		message = client_id + worker.MESSAGE_SEPARATOR + message
		err := f.Worker.SendMessage(message, send_queue_key)
		if err != nil {
			return err
		}
		log.Debugf("Sent message to output exchange: %s", message)
	}
	return nil
}

func (f *FilterByArgentina) CloseWorker() {
	if f.Worker != nil {
		f.Worker.CloseWorker()
	}
	log.Info("FilterByArgentina worker closed")
}
