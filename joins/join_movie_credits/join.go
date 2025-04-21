package join_movie_credits

import (
	worker "distribuidos-tp1/common/worker/worker"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("join_movie_credits")

type JoinMovieCreditsByIdConfig struct {
	worker.WorkerConfig
}

type JoinMovieCreditsById struct {
	worker.Worker
	messages_before_commit int
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE
// ---------------------------------
const ID = 0
const ACTORS = 1

func storeMovieWithId(lines []string, movies_by_id map[string]bool) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movies_by_id[parts[ID]] = true
	}
}

// ---------------------------------
// MESSAGE FORMAT: MOVIE_ID|ACTORS
// ---------------------------------

func joinMovieWithCredits(lines []string, movies_by_id map[string]bool) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_data := movies_by_id[parts[ID]]
		if !movie_data {
			continue
		}
		result = append(result, parts[ACTORS])
	}
	return result
}

func storeGroupedElements(results map[string]bool) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() map[string]bool {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewJoinMovieCreditsById(config JoinMovieCreditsByIdConfig, messages_before_commit int) *JoinMovieCreditsById {
	log.Infof("JoinMovieCreditsById: %+v", config)
	return &JoinMovieCreditsById{
		Worker: worker.Worker{
			InputExchange:       config.InputExchange,
			SecondInputExchange: config.SecondInputExchange,
			OutputExchange:      config.OutputExchange,
			MessageBroker:       config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
	}
}

func (f *JoinMovieCreditsById) RunWorker() error {
	log.Info("Starting JoinMovieCreditsById worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)
	worker.InitSecondReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	messages_before_commit := 0
	movies_by_id := make(map[string]bool)
	for message := range msgs {
		message := string(message.Body)
		if message == worker.MESSAGE_EOF {
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
		if message == worker.MESSAGE_EOF {
			err := worker.SendMessage(f.Worker, worker.MESSAGE_EOF)
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
			break
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		result := joinMovieWithCredits(lines, movies_by_id)
		message_to_send := strings.Join(result, "\n")
		if len(message_to_send) != 0 {
			err := worker.SendMessage(f.Worker, message_to_send)
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
		}
	}

	return nil
}

func (f *JoinMovieCreditsById) CloseWorker() error {
	err := worker.CloseSender(&f.Worker)
	if err != nil {
		return err
	}
	worker.CloseSecondReceiver(&f.Worker)
	return worker.CloseReceiver(&f.Worker)
}
