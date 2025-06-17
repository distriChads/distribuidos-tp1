package join_movie_credits

import (
	"context"
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"errors"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("join_movie_credits")

type JoinMovieCreditsByIdConfig struct {
	worker.WorkerConfig
}

type JoinMovieCreditsById struct {
	worker.Worker
	client_movies_by_id map[string]map[string]string
	received_movies     bool
	pending_credits     map[string][]string
	node_name           string
	log_replicas        int
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE
// ---------------------------------
const ID = 0
const ACTORS = 1

func storeMovieWithId(line string, movies_by_id map[string]string) {
	parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
	movies_by_id[parts[ID]] = parts[ID]
}

// ---------------------------------
// MESSAGE FORMAT: MOVIE_ID|ACTORS
// ---------------------------------

func joinMovieWithCredits(lines []string, movies_by_id map[string]string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_id := movies_by_id[parts[ID]]

		if len(movie_id) == 0 {
			continue
		}

		data := movie_id + worker.MESSAGE_SEPARATOR + parts[ACTORS]

		result = append(result, data)
	}
	return result
}

func NewJoinMovieCreditsById(config JoinMovieCreditsByIdConfig, node_name string) *JoinMovieCreditsById {
	log.Infof("JoinMovieCreditsById: %+v", config)
	replicas := 3
	grouped_elements, _ := common_statefull_worker.GetElements[string](node_name, replicas+1)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &JoinMovieCreditsById{
		Worker:              *worker,
		client_movies_by_id: grouped_elements,
		received_movies:     false,
		pending_credits:     make(map[string][]string),
		node_name:           node_name,
		log_replicas:        replicas,
	}
}

func (f *JoinMovieCreditsById) RunWorker(ctx context.Context, starting_message string) error {

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
				log.Warning("RECIBO EOF DE LAS MOVIES")
				pending_messages := f.pending_credits[client_id]
				for _, pending_message := range pending_messages {
					lines := strings.Split(strings.TrimSpace(pending_message), "\n")
					result := joinMovieWithCredits(lines, f.client_movies_by_id[client_id])
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
			common_statefull_worker.StoreElements(f.client_movies_by_id[client_id], client_id, f.node_name, f.log_replicas)
			msg.Ack(false)

		} else { // recibiendo credits
			if !f.received_movies {
				f.pending_credits[client_id] = append(f.pending_credits[client_id], message_str)
				// guardar estado
				msg.Ack(false)
				continue
			}

			if message_str == worker.MESSAGE_EOF {
				log.Warning("RECIBO EOF DE LOS CREDITS")
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
			result := joinMovieWithCredits(lines, f.client_movies_by_id[client_id])
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
