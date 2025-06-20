package group_by_overview_average

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"slices"
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
	messages_id            map[string][]string
}

type RevenueBudgetCount struct {
	Count               int
	Sum_revenue_average float64
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

func (g *GroupByOverviewAndAvg) HandleCommit(messages_before_commit int, client_id string, message_id string) {
	common_statefull_worker.StoreElementsWithMovies(g.grouped_elements[client_id], client_id, g.storage_base_dir, message_id)
}

func (g *GroupByOverviewAndAvg) MapToLines(client_id string) string {
	return mapToLines(g.grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]RevenueBudgetCount) string {
	var lines []string
	for overview, value := range grouped_elements {
		average := value.Sum_revenue_average
		line := fmt.Sprintf("%s%s%f%s%d", overview, worker.MESSAGE_SEPARATOR, average, worker.MESSAGE_SEPARATOR, value.Count)
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

func (g *GroupByOverviewAndAvg) UpdateState(lines []string, client_id string, message_id string) {
	if slices.Contains(g.messages_id[client_id], message_id) {
		log.Warning("Mensaje repetido")
		return
	}
	g.messages_id[client_id] = append(g.messages_id[client_id], message_id)
	groupByOverviewAndUpdate(lines, g.grouped_elements[client_id])
}

func groupByOverviewAndUpdate(lines []string, grouped_elements map[string]RevenueBudgetCount) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		budget, err := strconv.ParseFloat(parts[BUDGET], 64)
		if err != nil {
			continue
		}
		revenue, err := strconv.ParseFloat(parts[REVENUE], 64)
		if err != nil {
			continue
		}

		current := grouped_elements[parts[OVERVIEW]]
		current.Sum_revenue_average += revenue / budget
		current.Count += 1
		grouped_elements[parts[OVERVIEW]] = current

	}
}

func NewGroupByOverviewAndAvg(config GroupByOverviewAndAvgConfig, messages_before_commit int, storage_base_dir string) *GroupByOverviewAndAvg {
	log.Infof("GroupByOverviewAndAvg: %+v", config)

	grouped_elements, _, last_messages_in_state := common_statefull_worker.GetElements[RevenueBudgetCount](storage_base_dir)
	messages_id, last_messages_in_id := common_statefull_worker.GetIds(storage_base_dir)

	common_statefull_worker.RestoreStateIfNeeded(last_messages_in_state, last_messages_in_id, storage_base_dir)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &GroupByOverviewAndAvg{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		eofs:                   make(map[string]int),
		grouped_elements:       grouped_elements,
		storage_base_dir:       storage_base_dir,
		messages_id:            messages_id,
	}
}
