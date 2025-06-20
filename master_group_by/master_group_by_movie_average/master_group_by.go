package master_group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"slices"
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
	messages_id            map[string][]string
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

func (g *MasterGroupByMovieAndAvg) HandleCommit(messages_before_commit int, client_id string, message_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElementsWithMovies(g.grouped_elements[client_id], client_id, g.storage_base_dir, message_id)
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

func (g *MasterGroupByMovieAndAvg) UpdateState(lines []string, client_id string, message_id string) {
	if slices.Contains(g.messages_id[client_id], message_id) {
		log.Warning("Mensaje repetido")
		return
	}
	g.messages_id[client_id] = append(g.messages_id[client_id], message_id)
	groupByMovieAndUpdate(lines, g.grouped_elements[client_id])
}

// ---------------------------------
// MESSAGE FORMAT: TITLE|AVERAGE
// ---------------------------------
const TITLE = 0
const SCORE = 1

func groupByMovieAndUpdate(lines []string, grouped_elements map[string]ScoreAndCount) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		average, err := strconv.ParseFloat(parts[SCORE], 64)
		if err != nil {
			continue
		}
		current := grouped_elements[parts[TITLE]]
		current.score += average
		current.count += 1
		grouped_elements[parts[TITLE]] = current

	}
}

func NewGroupByMovieAndAvg(config MasterGroupByMovieAndAvgConfig, messages_before_commit int, expected_eof int, storage_base_dir string) *MasterGroupByMovieAndAvg {
	log.Infof("MasterGroupByMovieAndAvg: %+v", config)
	grouped_elements, _, last_messages_in_state := common_statefull_worker.GetElements[ScoreAndCount](storage_base_dir)
	messages_id, last_messages_in_id := common_statefull_worker.GetIds(storage_base_dir)

	common_statefull_worker.RestoreStateIfNeeded(last_messages_in_state, last_messages_in_id, storage_base_dir)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &MasterGroupByMovieAndAvg{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		expected_eof:           expected_eof,
		grouped_elements:       grouped_elements,
		eofs:                   make(map[string]int),
		storage_base_dir:       storage_base_dir,
		messages_id:            messages_id,
	}
}
