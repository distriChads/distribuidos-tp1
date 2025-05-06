package join_movie_ratings

import (
	worker "distribuidos-tp1/common/worker/worker"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("join_movie_ratings")

type JoinMovieRatingByIdConfig struct {
	worker.WorkerConfig
}

type JoinMovieRatingById struct {
	worker.Worker
	messages_before_commit int
	queue_to_send          int
	eofs                   map[string]int
	client_movies_by_id    map[string]map[string]string
	expected_eof           int
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE
// ---------------------------------
const ID = 0
const SCORE = 1

func storeMovieWithId(line string, movies_by_id map[string]string) {
	parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
	movies_by_id[parts[ID]] = line

}

// ---------------------------------
// MESSAGE FORMAT: MOVIE_ID|SCORE
// ---------------------------------

func joinMovieWithRating(lines []string, movies_by_id map[string]string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_data := movies_by_id[parts[ID]]
		if movie_data == "" {
			continue
		}
		result = append(result, movie_data+worker.MESSAGE_SEPARATOR+parts[SCORE])
	}
	return result
}

func storeGroupedElements(results map[string]string, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() map[string]string {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewJoinMovieRatingById(config JoinMovieRatingByIdConfig, messages_before_commit int, eof_counter int) *JoinMovieRatingById {
	log.Infof("JoinMovieRatingById: %+v", config)
	return &JoinMovieRatingById{
		Worker: worker.Worker{
			InputExchange:       config.InputExchange,
			SecondInputExchange: config.SecondInputExchange,
			OutputExchange:      config.OutputExchange,
			MessageBroker:       config.MessageBroker,
		},
		expected_eof:           eof_counter,
		messages_before_commit: messages_before_commit,
		eofs:                   make(map[string]int),
		client_movies_by_id:    make(map[string]map[string]string),
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
	for message := range msgs {
		message_str := string(message.Body)
		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[0]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[1]
		if _, ok := f.client_movies_by_id[client_id]; !ok {
			f.client_movies_by_id[client_id] = make(map[string]string)
		}
		if _, ok := f.eofs[client_id]; !ok {
			f.eofs[client_id] = 0
		}
		if message_str == worker.MESSAGE_EOF {
			f.eofs[client_id]++
			if f.eofs[client_id] >= f.expected_eof {
				delete(f.eofs, client_id)
				break
			}
			message.Ack(false)
			continue
		}
		messages_before_commit += 1
		line := strings.TrimSpace(message_str)
		storeMovieWithId(line, f.client_movies_by_id[client_id]) // ahora el filtro after 2000 envia de a una sola linea, por lo tanto puedo hacer esto
		if messages_before_commit >= f.messages_before_commit {
			storeGroupedElements(f.client_movies_by_id[client_id], client_id)
			messages_before_commit = 0
		}
		message.Ack(false)
	}
	log.Info("Finished first receiver")

	msgs, err = worker.SecondReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}

	for message := range msgs {
		message_str := string(message.Body)
		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[0]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[1]
		// de esta queue solamente recibo un EOF :)

		log.Debugf("Received message: %s", message_str)
		if message_str == worker.MESSAGE_EOF {
			delete(f.client_movies_by_id, client_id)
			for _, routing_key := range f.Worker.OutputExchange.RoutingKeys {
				key := routing_key
				message_to_send := client_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
				err := worker.SendMessage(f.Worker, message_to_send, key)
				if err != nil {
					log.Infof("Error sending message: %s", err.Error())
				}
			}
			break
		}
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		result := joinMovieWithRating(lines, f.client_movies_by_id[client_id])
		message_to_send := strings.Join(result, "\n")
		if len(message_to_send) != 0 {
			send_queue_key := f.Worker.OutputExchange.RoutingKeys[f.queue_to_send]
			message_to_send = client_id + worker.MESSAGE_SEPARATOR + message_to_send
			err := worker.SendMessage(f.Worker, message_to_send, send_queue_key)
			f.queue_to_send = (f.queue_to_send + 1) % len(f.Worker.OutputExchange.RoutingKeys)
			if err != nil {
				log.Infof("Error sending message: %s", err.Error())
			}
		}
		message.Ack(false)
	}
	log.Info("Finished second receiver")
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
