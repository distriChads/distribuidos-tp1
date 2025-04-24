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
	queue_to_send int
	eof_counter   int
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|GENRES|...
// ---------------------------------
const TITLE = 1
const DATE = 2
const COUNTRIES = 3
const GENRES = 4

func filterByCountrySpainAndOf2000(lines []string) []string {
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

func NewFilterBySpainAndOf2000(config FilterBySpainAndOf2000Config, eof_counter int) *FilterBySpainAndOf2000 {
	log.Infof("FilterBySpainAndOf2000: %+v", config)
	return &FilterBySpainAndOf2000{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		eof_counter: eof_counter,
	}
}

func (f *FilterBySpainAndOf2000) RunWorker() error {
	log.Info("Starting FilterByYear worker")
	worker.InitSender(&f.Worker)
	err := worker.InitReceiver(&f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}

	for message := range msgs {
		message := string(message.Body)
		if message == worker.MESSAGE_EOF {
			f.eof_counter--
			if f.eof_counter <= 0 {
				break
			}
			continue
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		filtered_lines := filterByCountrySpainAndOf2000(lines)
		message_to_send := strings.Join(filtered_lines, "\n")
		if len(message_to_send) != 0 {
			send_queue_key := f.Worker.OutputExchange.RoutingKeys[f.queue_to_send]
			err := worker.SendMessage(f.Worker, message_to_send, send_queue_key)
			f.queue_to_send = (f.queue_to_send + 1) % len(f.Worker.OutputExchange.RoutingKeys)
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
			log.Infof("Sent message %s to exchange %s (routing key: %s)", message_to_send, f.Worker.OutputExchange.Name, send_queue_key)
		}
	}

	log.Info("FilterBySpainAndOf2000 worker finished")
	return nil
}
