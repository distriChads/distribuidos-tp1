package join_movie_ratings

import (
	worker "distribuidos-tp1/common/worker/worker"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_after_2000")

type JoinMovieRatingByIdConfig struct {
	worker.WorkerConfig
}

type JoinMovieRatingById struct {
	worker.Worker
	messages_before_commit int
}

func storeMovieWithId(lines []string, movies_by_id map[string]string) {
	for _, line := range lines {
		parts := strings.Split(line, "|")
		movies_by_id[parts[0]] = line
	}
}

func joinMovieWithRating(lines []string, movies_by_id map[string]string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, "|")
		movie_data := movies_by_id[parts[0]]
		if movie_data == "" {
			continue
		}
		result = append(result, movie_data+"|"+parts[1])
	}
	return result
}

func storeGroupedElements(results map[string]string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() map[string]string {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewJoinMovieRatingById(config JoinMovieRatingByIdConfig, messages_before_commit int) *JoinMovieRatingById {
	log.Infof("JoinMovieRatingById: %+v", config)
	return &JoinMovieRatingById{
		Worker: worker.Worker{
			InputExchange:       config.InputExchange,
			SecondInputExchange: config.SecondInputExchange,
			OutputExchange:      config.OutputExchange,
			MessageBroker:       config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
	}
}

func (f *JoinMovieRatingById) RunWorker() error {
	log.Info("Starting JoinMovieRatingById worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)
	worker.InitSecondReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	messages_before_commit := 0
	movies_by_id := make(map[string]string)
	for message := range msgs {
		log.Infof("Received message in JOIN: %s", string(message.Body))
		message := string(message.Body)
		if message == "EOF" {
			break
		}
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message), "\n")
		storeMovieWithId(lines, movies_by_id)
		if messages_before_commit >= f.messages_before_commit {
			storeGroupedElements(movies_by_id)
			messages_before_commit = 0
		}
	}

	msgs, err = worker.SecondReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}

	for message := range msgs {
		message := string(message.Body)
		if message == "EOF" {
			err := worker.SendMessage(f.Worker, []byte("EOF"))
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
			break
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		result := joinMovieWithRating(lines, movies_by_id)
		message_to_send := strings.Join(result, "\n")
		if len(message_to_send) != 0 {
			err := worker.SendMessage(f.Worker, []byte(message_to_send))
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
		}
	}

	return nil
}

func (f *JoinMovieRatingById) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}
	worker.CloseSecondReceiver(&f.Worker)
	return worker.CloseReceiver(&f.Worker)
}
