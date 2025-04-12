package filters

import (
	worker "distribuidos-tp1/common/worker"
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
		parts := strings.Split(line, ",")
		raw_year := strings.Split(parts[1], "-")[0]
		year, err := strconv.Atoi(raw_year)
		if err != nil {
			continue
		}
		if !(year >= 2000 && year < 2010) {
			continue
		}
		countries := strings.Split(parts[0], "|")
		for _, country := range countries {
			if strings.TrimSpace(country) == "SPAIN" {
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
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}

	for message := range msgs {
		log.Infof("Received message: %s", string(message.Body))
		message := string(message.Body)
		lines := strings.Split(strings.TrimSpace(message), "\n")
		result := filterByCountrySpainAndOf2000(lines)
		var message_to_send []string
		for _, r := range result {
			parts := strings.Split(r, ",")
			title_and_id := strings.TrimSpace(parts[len(parts)-3]) + "," + strings.TrimSpace(parts[len(parts)-2])
			message_to_send = append(message_to_send, title_and_id)
		}
		err := worker.SendMessage(f.Worker, []byte(strings.Join(message_to_send, "\n")))
		if err != nil {
			log.Infof("Error sending message: %s", err.Error())
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
