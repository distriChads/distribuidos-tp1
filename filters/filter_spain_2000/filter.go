package filter_spain_2000

import (
	worker "distribuidos-tp1/common/worker/worker"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type FilterBySpainAndOf2000Config struct {
	worker.WorkerConfig
}

type FilterBySpainAndOf2000 struct {
	worker.Worker
}

var log = logging.MustGetLogger("filter_by_year")

func filterByCountrySpainAndOf2000(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, "|")
		raw_year := strings.Split(parts[2], "-")[0]
		year, err := strconv.Atoi(raw_year)
		if err != nil {
			continue
		}
		if !(year >= 2000 && year < 2010) {
			continue
		}
		countries := strings.Split(parts[3], ",")
		for _, country := range countries {
			if strings.TrimSpace(country) == "ES" {
				result = append(result, strings.TrimSpace(line))
				break
			}
		}

	}
	return result
}

func NewFilterBySpainAndOf2000(config FilterBySpainAndOf2000Config) *FilterBySpainAndOf2000 {
	log.Infof("FilterBySpainAndOf2000: %+v", config)
	return &FilterBySpainAndOf2000{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
	}
}

func (f *FilterBySpainAndOf2000) RunWorker() error {
	log.Info("Starting FilterByYear worker")
	err := worker.InitSender(&f.Worker)
	if err != nil {
		log.Errorf("Error initializing sender: %s", err.Error())
		return err
	}
	err = worker.InitReceiver(&f.Worker)
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
		log.Infof("Received message: %s", string(message.Body))
		message := string(message.Body)
		if message == "EOF" {
			break
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		result := filterByCountrySpainAndOf2000(lines)
		var message_buffer []string
		for _, r := range result {
			parts := strings.Split(r, "|")
			title_and_id := strings.TrimSpace(parts[1]) + "|" + strings.TrimSpace(parts[4])
			message_buffer = append(message_buffer, title_and_id)
		}
		message_to_send := strings.Join(message_buffer, "\n")
		if len(message_to_send) != 0 {
			err := worker.SendMessage(f.Worker, []byte(message_to_send))
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
		}

	}

	return nil
}

func (f *FilterBySpainAndOf2000) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
