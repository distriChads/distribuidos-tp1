package first_and_last

import (
	worker "distribuidos-tp1/common/worker/worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type FirstAndLastConfig struct {
	worker.WorkerConfig
}

type FirstAndLast struct {
	worker.Worker
}

var log = logging.MustGetLogger("first_and_last")

type MovieAvgByScore struct {
	Movie   string
	Average float64
}

func updateFirstAndLast(lines []string, first MovieAvgByScore, last MovieAvgByScore) (MovieAvgByScore, MovieAvgByScore) {
	for _, line := range lines {
		parts := strings.Split(line, ",")
		movie := parts[0]
		average, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		if first.Movie == "" && last.Movie == "" {
			first = MovieAvgByScore{Movie: movie, Average: average}
			last = MovieAvgByScore{Movie: movie, Average: average}
		} else {
			if average >= first.Average {
				first = MovieAvgByScore{Movie: movie, Average: average}
			} else if average <= last.Average {
				last = MovieAvgByScore{Movie: movie, Average: average}
			}
		}
	}
	return first, last
}

func NewFirstAndLast(config FirstAndLastConfig) *FirstAndLast {
	log.Infof("FirstAndLast: %+v", config)
	return &FirstAndLast{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
	}
}

func mapToLines(first MovieAvgByScore, last MovieAvgByScore) []string {
	var lines []string

	line := fmt.Sprintf("%s,%f", first.Movie, first.Average)
	lines = append(lines, line)
	line = fmt.Sprintf("%s,%f", last.Movie, last.Average)
	lines = append(lines, line)

	return lines
}

func (f *FirstAndLast) RunWorker() error {
	log.Info("Starting FirstAndLast worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	var first MovieAvgByScore
	var last MovieAvgByScore
	for message := range msgs {
		log.Infof("Received message in top five: %s", string(message.Body))
		message := string(message.Body)
		if message == "EOF" {
			break
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		first, last = updateFirstAndLast(lines, first, last)
	}
	message_to_send := mapToLines(first, last)
	err = worker.SendMessage(f.Worker, []byte(strings.Join(message_to_send, "\n")))
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	return nil
}

func (f *FirstAndLast) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
