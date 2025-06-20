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
	node_name              string
	messages_id            map[string][]string
}

type RevenueBudgetCount struct {
	count               int
	sum_revenue_average float64
}

var log = logging.MustGetLogger("group_by_overview_average")

func (g *GroupByOverviewAndAvg) NewClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]RevenueBudgetCount)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *GroupByOverviewAndAvg) ShouldCommit(messages_before_commit int, client_id string, message_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElementsWithMovies(g.grouped_elements[client_id], client_id, g.node_name, message_id)
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
		current.sum_revenue_average += revenue / budget
		current.count += 1
		grouped_elements[parts[OVERVIEW]] = current

	}
}

func NewGroupByOverviewAndAvg(config GroupByOverviewAndAvgConfig, messages_before_commit int, node_name string) *GroupByOverviewAndAvg {
	log.Infof("GroupByOverviewAndAvg: %+v", config)

	grouped_elements, _, last_messages_in_state := common_statefull_worker.GetElements[RevenueBudgetCount](node_name)
	messages_id, last_messages_in_id := common_statefull_worker.GetIds(node_name)

	common_statefull_worker.RestoreStateIfNeeded(last_messages_in_state, last_messages_in_id, node_name)
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
		node_name:              node_name,
		messages_id:            messages_id,
	}
}
