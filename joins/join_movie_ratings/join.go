package join_movie_ratings

import (
	"context"
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"errors"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("join_movie_ratings")

type JoinMovieRatingByIdConfig struct {
	worker.WorkerConfig
}

type JoinMovieRatingById struct {
	worker.Worker
	client_movies_by_id map[string]map[string]string
	received_movies     bool
	storage_base_dir    string
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

func NewJoinMovieRatingById(config JoinMovieRatingByIdConfig, storage_base_dir string) *JoinMovieRatingById {
	log.Infof("JoinMovieRatingById: %+v", config)
	grouped_elements, received_movies, _ := common_statefull_worker.GetElements[string](storage_base_dir)
	worker, err := worker.NewWorker(config.WorkerConfig, 1)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &JoinMovieRatingById{
		Worker:              *worker,
		client_movies_by_id: grouped_elements,
		received_movies:     received_movies,
		storage_base_dir:    storage_base_dir,
	}
}

func (f *JoinMovieRatingById) RunWorker(ctx context.Context, starting_message string) error {
	for {
		msg, inputIndex, err := f.Worker.ReceivedMessages(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("Shutting down message dispatcher gracefully")
				return nil
			}
			log.Errorf("Error receiving messages: %s", err)
			return err
		}
		message_str := string(msg.Body)

		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 3)[0]
		message_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 3)[1]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 3)[2]

		if inputIndex == 0 { // recibiendo movies
			if _, ok := f.client_movies_by_id[client_id]; !ok {
				f.client_movies_by_id[client_id] = make(map[string]string)
			}

			if message_str == worker.MESSAGE_EOF {
				f.received_movies = true
				msg.Ack(false)
				continue
			}

			line := strings.TrimSpace(message_str)
			storeMovieWithId(line, f.client_movies_by_id[client_id])
			common_statefull_worker.StoreElements(f.client_movies_by_id[client_id], client_id, f.storage_base_dir)
			msg.Ack(false)

		} else { // recibiendo credits
			if !f.received_movies {
				msg.Nack(false, true)
				continue
			}

			if message_str == worker.MESSAGE_EOF {
				log.Warning("RECIBO EOF DE LOS RATINGS")
				send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
				message_to_send := client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
				err := f.Worker.SendMessage(message_to_send, send_queue_key)
				if err != nil {
					log.Infof("Error sending message: %s", err.Error())
				}

				delete(f.client_movies_by_id, client_id)
				common_statefull_worker.CleanState(f.storage_base_dir, client_id)
				msg.Ack(false)
				continue
			}

			lines := strings.Split(strings.TrimSpace(message_str), "\n")
			result := joinMovieWithRating(lines, f.client_movies_by_id[client_id])
			message_to_send := strings.Join(result, "\n")
			if len(message_to_send) != 0 {
				send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
				message_to_send = client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + message_to_send
				err := f.Worker.SendMessage(message_to_send, send_queue_key)
				if err != nil {
					log.Infof("Error sending message: %s", err.Error())
				}
			}

			msg.Ack(false)
		}

	}
}
