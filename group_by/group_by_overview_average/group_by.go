package group_by_overview_average

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

// ---------------------------------
// MESSAGE FORMAT: id_client|MOVIE_ID|OVERVIEW|BUDGET|REVENUE
// ---------------------------------
const OVERVIEW = 1
const BUDGET = 2
const REVENUE = 3

type GroupByOverviewAndAvgConfig struct {
	worker.WorkerConfig
}

type GroupByOverviewAndAvg struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]RevenueBudgetCount
	eofs                   map[string]int
	storage_base_dir       string
	log_replicas           int
	movies_id              map[string][]string
}

type RevenueBudgetCount struct {
	count               int
	sum_revenue_average float64
}

var log = logging.MustGetLogger("group_by_overview_average")

func (g *GroupByOverviewAndAvg) EnsureClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]RevenueBudgetCount)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *GroupByOverviewAndAvg) HandleCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElementsWithMovies(g.grouped_elements[client_id], client_id, g.storage_base_dir, g.log_replicas, g.movies_id[client_id])
		return true
	}
	return false
}

func (g *GroupByOverviewAndAvg) MapToLines(client_id string) string {
	return mapToLines(g.grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]RevenueBudgetCount) string {
	var lines []string
	for overview, value := range grouped_elements {
		average := value.sum_revenue_average
		line := fmt.Sprintf("%s%s%f%s%d", overview, worker.MESSAGE_SEPARATOR, average, worker.MESSAGE_SEPARATOR, value.count)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *GroupByOverviewAndAvg) HandleEOF(client_id string) error {
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
	common_statefull_worker.CleanState(g.storage_base_dir, client_id)
	return nil
}

func (g *GroupByOverviewAndAvg) UpdateState(lines []string, client_id string) {
	g.movies_id[client_id] = groupByOverviewAndUpdate(lines, g.grouped_elements[client_id], g.movies_id[client_id])
}

func groupByOverviewAndUpdate(lines []string, grouped_elements map[string]RevenueBudgetCount, movies_id []string) []string {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movies_id = append(movies_id, parts[common_statefull_worker.MOVIE_ID])
		budget, err := strconv.ParseFloat(parts[BUDGET], 64)
		if err != nil {
			continue
		}
		revenue, err := strconv.ParseFloat(parts[REVENUE], 64)
		if err != nil {
			continue
		}

		current := grouped_elements[parts[OVERVIEW]]
		current.sum_revenue_average += revenue / budget
		current.count += 1
		grouped_elements[parts[OVERVIEW]] = current

	}
	return movies_id
}

func NewGroupByOverviewAndAvg(config GroupByOverviewAndAvgConfig, messages_before_commit int, storage_base_dir string) *GroupByOverviewAndAvg {
	log.Infof("GroupByOverviewAndAvg: %+v", config)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}
	replicas := 3
	grouped_elements, _, _ := common_statefull_worker.GetElements[RevenueBudgetCount](storage_base_dir, replicas+1)
	return &GroupByOverviewAndAvg{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		eofs:                   make(map[string]int),
		grouped_elements:       grouped_elements,
		storage_base_dir:       storage_base_dir,
		log_replicas:           replicas,
		movies_id:              make(map[string][]string),
	}
}
