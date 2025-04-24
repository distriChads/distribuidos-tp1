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

// ---------------------------------
// MESSAGE FORMAT: TITLE|AVERAGE
// ---------------------------------
const TITLE = 0
const AVERAGE = 1

func updateFirstAndLast(lines []string, first MovieAvgByScore, last MovieAvgByScore) (MovieAvgByScore, MovieAvgByScore) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie := parts[TITLE]
		average, err := strconv.ParseFloat(parts[AVERAGE], 64)
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

func mapToLines(first MovieAvgByScore, last MovieAvgByScore) string {
	var lines []string

	line := fmt.Sprintf("%s%s%f", first.Movie, worker.MESSAGE_SEPARATOR, first.Average)
	lines = append(lines, line)
	line = fmt.Sprintf("%s%s%f", last.Movie, worker.MESSAGE_SEPARATOR, last.Average)
	lines = append(lines, line)

	return strings.Join(lines, "\n")
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
		if message == worker.MESSAGE_EOF {
			break
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		first, last = updateFirstAndLast(lines, first, last)
	}
	message_to_send := mapToLines(first, last)
	log.Infof("First and Last: %s", message_to_send)
	send_queue_key := f.Worker.OutputExchange.RoutingKeys[0] // los topN son nodos unicos, y solo le envian al server
	err = worker.SendMessage(f.Worker, message_to_send, send_queue_key)
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}

	return nil
}
