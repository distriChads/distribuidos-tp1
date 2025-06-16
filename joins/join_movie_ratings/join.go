package join_movie_ratings

import (
	"context"
	worker "distribuidos-tp1/common/worker/worker"
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
	messages_before_commit int
	queue_to_send          int
	client_movies_by_id    map[string]map[string]string
	pending_ratings        map[string][]string
	received_movies        bool
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

func NewJoinMovieRatingById(config JoinMovieRatingByIdConfig) *JoinMovieRatingById {
	log.Infof("JoinMovieRatingById: %+v", config)

	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &JoinMovieRatingById{
		Worker:              *worker,
		client_movies_by_id: make(map[string]map[string]string),
		pending_ratings:     make(map[string][]string),
		received_movies:     false,
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
		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[0]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[1]

		if inputIndex == 0 { // recibiendo movies
			if _, ok := f.client_movies_by_id[client_id]; !ok {
				f.client_movies_by_id[client_id] = make(map[string]string)
			}

			if message_str == worker.MESSAGE_EOF {
				pending_messages := f.pending_ratings[client_id]
				for _, pending_message := range pending_messages {
					lines := strings.Split(strings.TrimSpace(pending_message), "\n")
					result := joinMovieWithRating(lines, f.client_movies_by_id[client_id])
					message_to_send := strings.Join(result, "\n")
					if len(message_to_send) != 0 {
						send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
						message_to_send = client_id + worker.MESSAGE_SEPARATOR + message_to_send
						err := f.Worker.SendMessage(message_to_send, send_queue_key)
						if err != nil {
							log.Infof("Error sending message: %s", err.Error())
						}
					}
				}
				f.received_movies = true

				// guardar estado
				msg.Ack(false)
				continue
			}

			line := strings.TrimSpace(message_str)
			storeMovieWithId(line, f.client_movies_by_id[client_id])
			msg.Ack(false)

		} else { // recibiendo credits
			if !f.received_movies {
				f.pending_ratings[client_id] = append(f.pending_ratings[client_id], message_str)
				// guardar estado
				msg.Ack(false)
				continue
			}

			if message_str == worker.MESSAGE_EOF {
				log.Warning("RECIBO EOF DE LOS RATINGS")
				send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
				message_to_send := client_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
				err := f.Worker.SendMessage(message_to_send, send_queue_key)
				if err != nil {
					log.Infof("Error sending message: %s", err.Error())
				}

				delete(f.client_movies_by_id, client_id)
				msg.Ack(false)
				continue
			}

			lines := strings.Split(strings.TrimSpace(message_str), "\n")
			result := joinMovieWithRating(lines, f.client_movies_by_id[client_id])
			message_to_send := strings.Join(result, "\n")
			if len(message_to_send) != 0 {
				send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
				message_to_send = client_id + worker.MESSAGE_SEPARATOR + message_to_send
				err := f.Worker.SendMessage(message_to_send, send_queue_key)
				if err != nil {
					log.Infof("Error sending message: %s", err.Error())
				}
			}

			msg.Ack(false)
		}

	}
}
