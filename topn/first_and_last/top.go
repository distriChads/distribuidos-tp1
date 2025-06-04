package first_and_last

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type FirstAndLastConfig struct {
	worker.WorkerConfig
}

type FirstAndLast struct {
	worker.Worker
	first_and_last_movies  map[string]FirstAndLastMovies
	messages_before_commit int
}

type MovieAvgByScore struct {
	Movie   string
	Average float64
}

type FirstAndLastMovies struct {
	first MovieAvgByScore
	last  MovieAvgByScore
}

var log = logging.MustGetLogger("first_and_last")

func (g *FirstAndLast) NewClient(client_id string) {
	if _, ok := g.first_and_last_movies[client_id]; !ok {
		g.first_and_last_movies[client_id] = FirstAndLastMovies{}
	}
}

func (g *FirstAndLast) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		storeGroupedElements(g.first_and_last_movies[client_id], client_id)
		return true
	}
	return false
}

func (g *FirstAndLast) MapToLines(client_id string) string {
	return mapToLines(g.first_and_last_movies[client_id])
}

func mapToLines(first_and_last_movies FirstAndLastMovies) string {
	var lines []string

	line := fmt.Sprintf("%s%s%f", first_and_last_movies.first.Movie, worker.MESSAGE_SEPARATOR, first_and_last_movies.first.Average)
	lines = append(lines, line)
	line = fmt.Sprintf("%s%s%f", first_and_last_movies.last.Movie, worker.MESSAGE_SEPARATOR, first_and_last_movies.last.Average)
	lines = append(lines, line)

	return strings.Join(lines, "\n")
}

func (g *FirstAndLast) HandleEOF(client_id string) error {
	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	delete(g.first_and_last_movies, client_id)
	return nil
}

func (g *FirstAndLast) UpdateState(lines []string, client_id string) {
	g.first_and_last_movies[client_id] = updateFirstAndLast(lines, g.first_and_last_movies[client_id])
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

		if firstAndLastMovies.first.Movie == "" && firstAndLastMovies.last.Movie == "" {
			firstAndLastMovies.first = MovieAvgByScore{Movie: movie, Average: average}
			firstAndLastMovies.last = MovieAvgByScore{Movie: movie, Average: average}
		} else {
			if average >= firstAndLastMovies.first.Average {
				firstAndLastMovies.first = MovieAvgByScore{Movie: movie, Average: average}
			} else if average <= firstAndLastMovies.last.Average {
				firstAndLastMovies.last = MovieAvgByScore{Movie: movie, Average: average}
			}
		}
	}
	return firstAndLastMovies
}

func storeGroupedElements(results FirstAndLastMovies, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() FirstAndLastMovies {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return FirstAndLastMovies{}
}

func NewFirstAndLast(config FirstAndLastConfig, messages_before_commit int) *FirstAndLast {
	log.Infof("FirstAndLast: %+v", config)
	return &FirstAndLast{
		Worker: worker.Worker{
			MessageBroker: config.MessageBroker,
		},
		first_and_last_movies:  make(map[string]FirstAndLastMovies, 0),
		messages_before_commit: messages_before_commit,
	}
}

func (g *FirstAndLast) RunWorker(starting_message string) error {
	msgs, err := common_statefull_worker.Init(&g.Worker, starting_message)
	if err != nil {
		return err
	}
	return common_statefull_worker.RunWorker(g, msgs)
}
