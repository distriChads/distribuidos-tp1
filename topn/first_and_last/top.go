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
	firstAndLastMovies map[string]FirstAndLastMovies
}

var log = logging.MustGetLogger("first_and_last")

type MovieAvgByScore struct {
	Movie   string
	Average float64
}

type FirstAndLastMovies struct {
	first MovieAvgByScore
	last  MovieAvgByScore
}

// ---------------------------------
// MESSAGE FORMAT: TITLE|AVERAGE
// ---------------------------------
const TITLE = 0
const AVERAGE = 1

func updateFirstAndLast(lines []string, firstAndLastMovies FirstAndLastMovies) FirstAndLastMovies {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie := parts[TITLE]
		average, err := strconv.ParseFloat(parts[AVERAGE], 64)
		if err != nil {
			continue
		}

		if firstAndLastMovies.first.Movie == "" && firstAndLastMovies.last.Movie == "" {
			firstAndLastMovies.first = MovieAvgByScore{Movie: movie, Average: average}
			firstAndLastMovies.last = MovieAvgByScore{Movie: movie, Average: average}
		} else {
			if average >= firstAndLastMovies.first.Average {
				firstAndLastMovies.first = MovieAvgByScore{Movie: movie, Average: average}
			} else if average <= firstAndLastMovies.last.Average {
				firstAndLastMovies.last = MovieAvgByScore{Movie: movie, Average: average}
			}
		}
	}
	return firstAndLastMovies
}

func NewFirstAndLast(config FirstAndLastConfig) *FirstAndLast {
	log.Infof("FirstAndLast: %+v", config)
	return &FirstAndLast{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		firstAndLastMovies: make(map[string]FirstAndLastMovies),
	}
}

func mapToLines(firstAndLastMovies FirstAndLastMovies) string {
	var lines []string

	line := fmt.Sprintf("%s%s%f", firstAndLastMovies.first.Movie, worker.MESSAGE_SEPARATOR, firstAndLastMovies.first.Average)
	lines = append(lines, line)
	line = fmt.Sprintf("%s%s%f", firstAndLastMovies.last.Movie, worker.MESSAGE_SEPARATOR, firstAndLastMovies.last.Average)
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
	for message := range msgs {
		message_str := string(message.Body)
		client_id := strings.Split(message_str, "/")[0]
		message_str = strings.Split(message_str, "/")[1]
		if _, ok := f.firstAndLastMovies[client_id]; !ok {
			f.firstAndLastMovies[client_id] = FirstAndLastMovies{}
		}
		log.Debugf("Received message: %s", message_str)
		if strings.TrimSpace(message_str) == worker.MESSAGE_EOF {
			log.Infof("Sending result for client %s", client_id)
			sendResult(f, client_id)
			delete(f.firstAndLastMovies, client_id)
			log.Infof("Client %s finished", client_id)
			message.Ack(false)
			continue
		}
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		f.firstAndLastMovies[client_id] = updateFirstAndLast(lines, f.firstAndLastMovies[client_id])
		message.Ack(false)
	}

	return nil
}

func sendResult(f *FirstAndLast, client_id string) error {
	message_to_send := client_id + "/" + mapToLines(f.firstAndLastMovies[client_id])
	send_queue_key := f.Worker.OutputExchange.RoutingKeys[0] 
	err := worker.SendMessage(f.Worker, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	message_to_send = client_id + "/" + worker.MESSAGE_EOF
	err = worker.SendMessage(f.Worker, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	return nil
}
