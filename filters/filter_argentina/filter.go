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
	queue_to_send int
	eof_counter   int
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|...
// ---------------------------------
const COUNTRIES = 3

func filterByArgentina(lines []string) []string {
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

func NewFilterByArgentina(config FilterByArgentinaConfig, eof_counter int) *FilterByArgentina {
	log.Infof("NewFilterByYear: %+v", config)
	return &FilterByArgentina{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		eof_counter: eof_counter,
	}
}

func (f *FilterByArgentina) RunWorker() error {
	log.Info("Starting FilterByYear worker")
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
			f.eof_counter--
			if f.eof_counter <= 0 {
				for _, queue_name := range f.Worker.OutputExchange.RoutingKeys {
					err := worker.SendMessage(f.Worker, worker.MESSAGE_EOF, queue_name)
					if err != nil {
						log.Infof("Error sending message: %s", err.Error())
					}
				}
				message.Ack(false)
				break
			}
			message.Ack(false)
			continue
		}
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		filtered_lines := filterByArgentina(lines)
		message_to_send := strings.Join(filtered_lines, "\n")
		if len(message_to_send) != 0 {
			send_queue_key := f.Worker.OutputExchange.RoutingKeys[f.queue_to_send]
			err := worker.SendMessage(f.Worker, message_to_send, send_queue_key)
			f.queue_to_send = (f.queue_to_send + 1) % len(f.Worker.OutputExchange.RoutingKeys)
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
			log.Debugf("Sent message to output exchange: %s", message_to_send)
		}
		message.Ack(false)
	}

	log.Info("FilterByArgentina worker finished")
	return nil
}
