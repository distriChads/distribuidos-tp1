package group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type GroupByMovieAndAvgConfig struct {
	worker.WorkerConfig
}

type GroupByMovieAndAvg struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]ScoreAndCount
	eofs                   map[string]int
	node_name              string
	log_replicas           int
}

type ScoreAndCount struct {
	count int
	score float64
}

var log = logging.MustGetLogger("group_by_movie_average")

func (g *GroupByMovieAndAvg) NewClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]ScoreAndCount)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *GroupByMovieAndAvg) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElements(g.grouped_elements[client_id], client_id, g.node_name, g.log_replicas)
		return true
	}
	return false
}

func (g *GroupByMovieAndAvg) MapToLines(client_id string) string {
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

func (g *GroupByMovieAndAvg) HandleEOF(client_id string) error {
	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	delete(g.grouped_elements, client_id)
	delete(g.eofs, client_id)
	return nil
}

func (g *GroupByMovieAndAvg) UpdateState(lines []string, client_id string) {
	groupByMovieAndUpdate(lines, g.grouped_elements[client_id])
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|SCORE
// ---------------------------------
const TITLE = 1
const SCORE = 2

func groupByMovieAndUpdate(lines []string, grouped_elements map[string]ScoreAndCount) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		score, err := strconv.ParseFloat(parts[SCORE], 64)
		if err != nil {
			continue
		}

		current := grouped_elements[parts[TITLE]]
		current.score += score
		current.count += 1
		grouped_elements[parts[TITLE]] = current

	}
	log.Debugf("Grouped elements: %+v", grouped_elements)
}

func NewGroupByMovieAndAvg(config GroupByMovieAndAvgConfig, messages_before_commit int, node_name string) *GroupByMovieAndAvg {
	log.Infof("GroupByMovieAndAvg: %+v", config)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}
	replicas := 3
	grouped_elements, _ := common_statefull_worker.GetElements[ScoreAndCount](node_name, replicas+1)
	return &GroupByMovieAndAvg{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		eofs:                   make(map[string]int),
		grouped_elements:       grouped_elements,
		node_name:              node_name,
		log_replicas:           replicas,
	}
}
