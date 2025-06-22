package first_and_last

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"strconv"
	"strings"

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
	err := common_statefull_worker.SendResult(g.Worker, client_id, g.MapToLines(client_id))
	if err != nil {
		return err
	}
	delete(g.first_and_last_movies, client_id)
	common_statefull_worker.CleanState(g.storage_base_dir, client_id)
	return nil
}

func (g *FirstAndLast) UpdateState(lines []string, client_id string, message_id string) {
	g.first_and_last_movies[client_id][client_id] = updateFirstAndLast(lines, g.first_and_last_movies[client_id][client_id])
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
	grouped_elements, _, _ := common_statefull_worker.GetElements[FirstAndLastMovies](storage_base_dir)
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
	}
}
