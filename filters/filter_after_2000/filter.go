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
	eof_counter int
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|...
// ---------------------------------
const ID = 0
const TITLE = 1
const DATE = 2

func filterByYearAfter2000(lines []string) []string {
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

func (f *FilterByAfterYear2000) RunWorker() error {
	log.Info("Starting FilterByAfterYear2000 worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}

	for message := range msgs {
		message_str := string(message.Body)
		log.Debugf("Received message: %s", message_str)
		if message_str == worker.MESSAGE_EOF {
			log.Info("Received EOF")
			f.eof_counter--
			if f.eof_counter <= 0 {
				log.Info("Sending EOF")
				for _, queue_name := range f.Worker.OutputExchange.RoutingKeys {
					err := worker.SendMessage(f.Worker, worker.MESSAGE_EOF, queue_name)
					if err != nil {
						log.Infof("Error sending message: %s", err.Error())
					}
				}
				log.Info("Finished sending EOF")
				message.Ack(false)
				break
			}
			message.Ack(false)
			continue
		}
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		filtered_lines := filterByYearAfter2000(lines)
		for _, line := range filtered_lines {
			parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
			message_to_send := line
			if len(message_to_send) != 0 {
				id, err := strconv.Atoi(parts[ID])
				if err != nil {
					continue
				}
				send_queue_key := f.Worker.OutputExchange.RoutingKeys[id%len(f.Worker.OutputExchange.RoutingKeys)]
				err = worker.SendMessage(f.Worker, message_to_send, send_queue_key)
				if err != nil {
					log.Infof("Error sending message: %s", err.Error())
				}
			}
		}
		message.Ack(false)
	}

	return nil
}
