package group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type GroupByMovieAndAvgConfig struct {
	worker.WorkerConfig
}

type GroupByMovieAndAvg struct {
	worker.Worker
	messages_before_commit int
}

type ScoreAndCount struct {
	count int
	score float64
}

var log = logging.MustGetLogger("group_by_movie_average")

func groupByMovieAndUpdate(lines []string, grouped_elements map[string]ScoreAndCount) {
	for _, line := range lines {
		parts := strings.Split(line, "|")
		score, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			continue
		}

		current := grouped_elements[parts[1]]
		current.score += score
		current.count += 1
		grouped_elements[parts[1]] = current

	}
}

func storeGroupedElements(results map[string]ScoreAndCount) {
	// TODO: Dumpear el hashmap a un archivo
}

func mapToLines(grouped_elements map[string]ScoreAndCount) []string {
	var lines []string
	for title, value := range grouped_elements {
		average := value.score / float64(value.count)
		line := fmt.Sprintf("%s,%f", title, average)
		lines = append(lines, line)
	}
	return lines
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByMovieAndAvg(config GroupByMovieAndAvgConfig, messages_before_commit int) *GroupByMovieAndAvg {
	log.Infof("GroupByMovieAndAvg: %+v", config)
	return &GroupByMovieAndAvg{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
	}
}

func (f *GroupByMovieAndAvg) RunWorker() error {
	log.Info("Starting GroupByMovieAndAvg worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	messages_before_commit := 0
	grouped_elements := make(map[string]ScoreAndCount)
	for message := range msgs {
		log.Infof("Received message in group by title and avg: %s", string(message.Body))
		message := string(message.Body)
		if message == "EOF" {
			break
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message), "\n")
		groupByMovieAndUpdate(lines, grouped_elements)
		if messages_before_commit >= f.messages_before_commit {
			storeGroupedElements(grouped_elements)
			messages_before_commit = 0
		}
	}
	message_to_send := mapToLines(grouped_elements)
	err = worker.SendMessage(f.Worker, []byte(strings.Join(message_to_send, "\n")))
	// TODO: Enviar a una cola de un agrupador "maestro" que haga la ultima agrupacion y este se lo envie al proximo chavoncito
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	err = worker.SendMessage(f.Worker, []byte("EOF"))
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	return nil
}

func (f *GroupByMovieAndAvg) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}

	return worker.CloseReceiver(&f.Worker)
}
