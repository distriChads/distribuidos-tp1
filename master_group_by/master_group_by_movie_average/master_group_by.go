package master_group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type MasterGroupByMovieAndAvgConfig struct {
	worker.WorkerConfig
}

type MasterGroupByMovieAndAvg struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]ScoreAndCount
	eofs                   map[string]int
	storage_base_dir       string
	log_replicas           int
	movies_id              map[string][]string
}

type ScoreAndCount struct {
	count int
	score float64
}

var log = logging.MustGetLogger("master_group_by_movie_average")

func (g *MasterGroupByMovieAndAvg) EnsureClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]ScoreAndCount)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *MasterGroupByMovieAndAvg) HandleCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElementsWithMovies(g.grouped_elements[client_id], client_id, g.storage_base_dir, g.log_replicas, g.movies_id[client_id])
		return true
	}
	return false
}

func (g *MasterGroupByMovieAndAvg) MapToLines(client_id string) string {
	return mapToLines(g.grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]ScoreAndCount) string {
	var lines []string
	for title, value := range grouped_elements {
		average := value.score / float64(value.count)
		line := fmt.Sprintf("%s%s%f", title, worker.MESSAGE_SEPARATOR, average)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *MasterGroupByMovieAndAvg) HandleEOF(client_id string) error {
	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	delete(g.grouped_elements, client_id)
	delete(g.eofs, client_id)
	common_statefull_worker.CleanState(g.storage_base_dir, client_id)
	return nil
}

func (g *MasterGroupByMovieAndAvg) UpdateState(lines []string, client_id string) {
	g.movies_id[client_id] = groupByMovieAndUpdate(lines, g.grouped_elements[client_id], g.movies_id[client_id])
}

// ---------------------------------
// MESSAGE FORMAT: TITLE|AVERAGE
// ---------------------------------
const TITLE = 0
const SCORE = 1

func groupByMovieAndUpdate(lines []string, grouped_elements map[string]ScoreAndCount, movies_id []string) []string {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movies_id = append(movies_id, parts[common_statefull_worker.MOVIE_ID])
		average, err := strconv.ParseFloat(parts[SCORE], 64)
		if err != nil {
			continue
		}
		current := grouped_elements[parts[TITLE]]
		current.score += average
		current.count += 1
		grouped_elements[parts[TITLE]] = current

	}
	return movies_id
}

func NewGroupByMovieAndAvg(config MasterGroupByMovieAndAvgConfig, messages_before_commit int, expected_eof int, storage_base_dir string) *MasterGroupByMovieAndAvg {
	log.Infof("MasterGroupByMovieAndAvg: %+v", config)

	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	replicas := 3
	grouped_elements, _, _ := common_statefull_worker.GetElements[ScoreAndCount](storage_base_dir, replicas+1)
	return &MasterGroupByMovieAndAvg{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		expected_eof:           expected_eof,
		grouped_elements:       grouped_elements,
		eofs:                   make(map[string]int),
		storage_base_dir:       storage_base_dir,
		log_replicas:           replicas,
		movies_id:              make(map[string][]string),
	}
}
