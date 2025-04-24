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
	queue_to_send          int
	eof_counter            int
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE
// ---------------------------------
const ID = 0
const ACTORS = 1

func storeMovieWithId(line string, movies_by_id map[string]bool) {
	parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
	movies_by_id[parts[ID]] = true
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

func NewJoinMovieCreditsById(config JoinMovieCreditsByIdConfig, messages_before_commit int, eof_counter int) *JoinMovieCreditsById {
	log.Infof("JoinMovieCreditsById: %+v", config)
	return &JoinMovieCreditsById{
		Worker: worker.Worker{
			InputExchange:       config.InputExchange,
			SecondInputExchange: config.SecondInputExchange,
			OutputExchange:      config.OutputExchange,
			MessageBroker:       config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		eof_counter:            eof_counter,
	}
}

func (f *JoinMovieCreditsById) RunWorker() error {
	log.Info("Starting JoinMovieCreditsById worker")
	worker.InitSender(&f.Worker)
	err := worker.InitReceiver(&f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	err = worker.InitSecondReceiver(&f.Worker)
	if err != nil {
		log.Errorf("Error initializing second receiver: %s", err.Error())
		return err
	}
	log.Infof("JoinMovieCreditsById worker initialized")

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
			f.eof_counter--
			if f.eof_counter <= 0 {
				break
			}
			continue
		}
		messages_before_commit += 1
		line := strings.TrimSpace(message)
		storeMovieWithId(line, movies_by_id) // ahora el filtro after 2000 envia de a una sola linea, por lo tanto puedo hacer esto
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
	i := 0
	for message := range msgs {
		log.Debugf("Batch received Number: %d", i)
		message := string(message.Body)
		if message == worker.MESSAGE_EOF {
			for _, queue_name := range f.Worker.OutputExchange.RoutingKeys {
				err := worker.SendMessage(f.Worker, worker.MESSAGE_EOF, queue_name)
				if err != nil {
					log.Infof("Error sending message: %s", err.Error())
				}
			}
			break
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		result := joinMovieWithCredits(lines, movies_by_id)
		message_to_send := strings.Join(result, "\n")
		if len(message_to_send) != 0 {
			send_queue_key := f.Worker.OutputExchange.RoutingKeys[f.queue_to_send]
			err := worker.SendMessage(f.Worker, message_to_send, send_queue_key)
			f.queue_to_send = (f.queue_to_send + 1) % len(f.Worker.OutputExchange.RoutingKeys)
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
