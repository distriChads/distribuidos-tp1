package group_by_country_sum

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type GroupByCountryAndSumConfig struct {
	worker.WorkerConfig
}

type GroupByCountryAndSum struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]int
	eofs                   map[string]int
	node_name              string
	log_replicas           int
	movies_id              map[string][]string
}

var log = logging.MustGetLogger("group_by_country_sum")

func (g *GroupByCountryAndSum) NewClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]int)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *GroupByCountryAndSum) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElementsWithMovies(g.grouped_elements[client_id], client_id, g.node_name, g.log_replicas, g.movies_id[client_id])
		return true
	}
	return false
}

func (g *GroupByCountryAndSum) MapToLines(client_id string) string {
	return mapToLines(g.grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]int) string {
	var lines []string
	for country, budget := range grouped_elements {
		line := fmt.Sprintf("%s%s%d", country, worker.MESSAGE_SEPARATOR, budget)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *GroupByCountryAndSum) HandleEOF(client_id string) error {
	// g.eofs[client_id]++
	// if g.eofs[client_id] >= g.expected_eof {
	// 	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	delete(g.grouped_elements, client_id)
	// 	delete(g.eofs, client_id)
	// }
	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	delete(g.grouped_elements, client_id)
	delete(g.eofs, client_id)
	return nil
}

func (g *GroupByCountryAndSum) UpdateState(lines []string, client_id string) {
	g.movies_id[client_id] = groupByCountryAndSum(lines, g.grouped_elements[client_id], g.movies_id[client_id])
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRY|GENRES|BUDGET
// ---------------------------------
const COUNTRY = 3
const BUDGET = 5

func groupByCountryAndSum(lines []string, grouped_elements map[string]int, movies_id []string) []string {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movies_id = append(movies_id, parts[common_statefull_worker.MOVIE_ID])
		budget, err := strconv.Atoi(parts[BUDGET])
		if err != nil {
			continue
		}
		grouped_elements[parts[COUNTRY]] += budget
	}
	return movies_id
}

func NewGroupByCountryAndSum(config GroupByCountryAndSumConfig, messages_before_commit int, node_name string) *GroupByCountryAndSum {
	log.Infof("GroupByCountryAndSum: %+v", config)
	replicas := 3
	grouped_elements, _, _ := common_statefull_worker.GetElements[int](node_name, replicas+1)

	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &GroupByCountryAndSum{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		grouped_elements:       grouped_elements,
		eofs:                   make(map[string]int),
		log_replicas:           3,
		movies_id:              make(map[string][]string),
	}
}
