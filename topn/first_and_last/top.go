package first_and_last

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type FirstAndLastConfig struct {
	worker.WorkerConfig
}

type FirstAndLast struct {
	worker.Worker
	first_and_last_movies  map[string]map[string]FirstAndLastMovies
	messages_before_commit int
	storage_base_dir       string
	eof_id                 map[string]string
	node_id                map[string]string
}

type MovieAvgByScore struct {
	Movie   string
	Average float64
}

type FirstAndLastMovies struct {
	First MovieAvgByScore
	Last  MovieAvgByScore
}

var log = logging.MustGetLogger("first_and_last")

func (g *FirstAndLast) EnsureClient(client_id string) {
	if _, ok := g.first_and_last_movies[client_id]; !ok {
		g.first_and_last_movies[client_id] = make(map[string]FirstAndLastMovies)
	}
	if _, ok := g.first_and_last_movies[client_id][client_id]; !ok {
		g.first_and_last_movies[client_id][client_id] = FirstAndLastMovies{}
	}

	if _, ok := g.eof_id[client_id]; !ok {
		ids_to_append := make([]string, 2)
		message_id, err := uuid.NewRandom()
		if err != nil {
			log.Errorf("Error generating uuid: %s", err.Error())
			return
		}
		ids_to_append[0] = message_id.String()
		g.eof_id[client_id] = message_id.String()
		message_id, err = uuid.NewRandom()
		if err != nil {
			log.Errorf("Error generating uuid: %s", err.Error())
			return
		}
		ids_to_append[1] = message_id.String()
		g.node_id[client_id] = message_id.String()
		common_statefull_worker.AppendMyId(g.storage_base_dir, ids_to_append, client_id)
	}

}

func (g *FirstAndLast) HandleCommit(client_id string, message amqp091.Delivery) error {

	err := common_statefull_worker.StoreElements(g.first_and_last_movies[client_id], client_id, g.storage_base_dir)
	if err != nil {
		return err
	}
	message.Ack(false)
	return nil
}

func (g *FirstAndLast) MapToLines(client_id string) string {
	return mapToLines(g.first_and_last_movies[client_id][client_id])
}

func mapToLines(first_and_last_movies FirstAndLastMovies) string {
	var lines []string

	line := fmt.Sprintf("%s%s%f", first_and_last_movies.First.Movie, worker.MESSAGE_SEPARATOR, first_and_last_movies.First.Average)
	lines = append(lines, line)
	line = fmt.Sprintf("%s%s%f", first_and_last_movies.Last.Movie, worker.MESSAGE_SEPARATOR, first_and_last_movies.Last.Average)
	lines = append(lines, line)

	return strings.Join(lines, "\n")
}

func (g *FirstAndLast) HandleEOF(client_id string, message_id string) error {
	err := common_statefull_worker.SendResult(g.Worker, client_id, g.MapToLines(client_id), g.node_id[client_id], g.eof_id[client_id])
	if err != nil {
		return err
	}
	delete(g.first_and_last_movies, client_id)
	common_statefull_worker.CleanState(g.storage_base_dir, client_id)
	return nil
}

func (g *FirstAndLast) UpdateState(lines []string, client_id string, message_id string) bool {
	g.first_and_last_movies[client_id][client_id] = updateFirstAndLast(lines, g.first_and_last_movies[client_id][client_id])
	return false
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

		if firstAndLastMovies.First.Movie == "" && firstAndLastMovies.Last.Movie == "" {
			firstAndLastMovies.First = MovieAvgByScore{Movie: movie, Average: average}
			firstAndLastMovies.Last = MovieAvgByScore{Movie: movie, Average: average}
		} else {
			if average >= firstAndLastMovies.First.Average {
				firstAndLastMovies.First = MovieAvgByScore{Movie: movie, Average: average}
			} else if average <= firstAndLastMovies.Last.Average {
				firstAndLastMovies.Last = MovieAvgByScore{Movie: movie, Average: average}
			}
		}
	}
	return firstAndLastMovies
}

func NewFirstAndLast(config FirstAndLastConfig, messages_before_commit int, storage_base_dir string) *FirstAndLast {
	log.Infof("FirstAndLast: %+v", config)
	grouped_elements, _ := common_statefull_worker.GetElements[FirstAndLastMovies](storage_base_dir)

	my_id, _ := common_statefull_worker.GetMyId(storage_base_dir)
	eof_id := make(map[string]string)
	node_id := make(map[string]string)
	for key, val := range my_id {
		if len(val) == 2 {
			eof_id[key] = val[0]
			node_id[key] = val[1]
		}
	}

	worker, err := worker.NewWorker(config.WorkerConfig, 1)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &FirstAndLast{
		Worker:                 *worker,
		first_and_last_movies:  grouped_elements,
		messages_before_commit: messages_before_commit,
		storage_base_dir:       storage_base_dir,
		eof_id:                 eof_id,
		node_id:                node_id,
	}
}
