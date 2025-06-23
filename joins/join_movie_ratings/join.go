package join_movie_ratings

import (
	"context"
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"distribuidos-tp1/joins/common_join"
	"errors"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("join_movie_ratings")

type JoinMovieRatingByIdConfig struct {
	worker.WorkerConfig
}

type JoinMovieRatingById struct {
	*common_join.CommonJoin
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

func (f *JoinMovieRatingById) joinMovieWithRating(lines []string, movies_by_id map[string]string) {

	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_data := movies_by_id[parts[ID]]
		if movie_data == "" {
			continue
		}
		id, err := strconv.Atoi(parts[ID])
		if err != nil {
			continue
		}
		f.Buffer.AddMessage(id, movie_data+worker.MESSAGE_SEPARATOR+parts[SCORE])
	}
}

func NewJoinMovieRatingById(config JoinMovieRatingByIdConfig, storage_base_dir string, eofCounter int) *JoinMovieRatingById {
	join := common_join.NewCommonJoin(config.WorkerConfig, storage_base_dir, eofCounter)
	return &JoinMovieRatingById{
		CommonJoin: join,
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
			f.EnsureClient(client_id)

			if message_str == worker.MESSAGE_EOF {
				err := f.HandleMovieEOF(client_id, message_id)
				if err != nil {
					return err
				}
				msg.Ack(false)
				continue
			}

			line := strings.TrimSpace(message_str)
			storeMovieWithId(line, f.Client_movies_by_id[client_id])
			err := common_statefull_worker.StoreElementsWithBoolean(f.Client_movies_by_id[client_id], client_id, f.Storage_base_dir, f.Received_movies)
			if err != nil {
				return err
			}
			msg.Ack(false)

		} else { // recibiendo ratings
			if !f.Received_movies {
				if message_str == worker.MESSAGE_EOF {
					msg.Nack(false, true)
				}
				f.HandlePending(client_id, message_id, message_str)
				msg.Ack(false)
				continue
			}

			if len(f.Pending[client_id]) != 0 {
				pending_messages := f.Pending[client_id]
				for _, pending_message := range pending_messages {
					err := f.HandleLine(client_id, message_id, pending_message, f.joinMovieWithRating)
					if err != nil {
						return err
					}
				}
				common_statefull_worker.CleanPending(f.Storage_base_dir, client_id)
				delete(f.Pending, client_id)
			}

			if message_str == worker.MESSAGE_EOF {
				err := f.HandleJoiningEOF(client_id, message_id)
				if err != nil {
					return err
				}
				msg.Ack(false)
				continue
			}

			err = f.HandleLine(client_id, message_id, message_str, f.joinMovieWithRating)
			if err != nil {
				return err
			}
			msg.Ack(false)
		}

	}
}
