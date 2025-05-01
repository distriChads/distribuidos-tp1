package group_by_overview_average

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type GroupByOverviewAndAvgConfig struct {
	worker.WorkerConfig
}

var log = logging.MustGetLogger("group_by_overview_average")

type GroupByOverviewAndAvg struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]RevenueBudgetCount
	eofs                   map[string]int
}

type RevenueBudgetCount struct {
	count   int
	revenue float64
	budget  float64
}

func (g *GroupByOverviewAndAvg) NewClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]RevenueBudgetCount)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *GroupByOverviewAndAvg) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		storeGroupedElements(g.grouped_elements[client_id], client_id)
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
		result := 0.0
		if value.budget > 0 {
			result = value.revenue / value.budget
		}
		average := result / float64(value.count)
		line := fmt.Sprintf("%s%s%f", overview, worker.MESSAGE_SEPARATOR, average)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *GroupByOverviewAndAvg) HandleEOF(client_id string) error {
	g.eofs[client_id]++
	if g.eofs[client_id] >= g.expected_eof {
		err := common_group_by.SendResult(g.Worker, g, client_id)
		if err != nil {
			return err
		}
		delete(g.grouped_elements, client_id)
		delete(g.eofs, client_id)
	}
	return nil
}

func (g *GroupByOverviewAndAvg) GroupByAndUpdate(lines []string, client_id string) {
	groupByOverviewAndUpdate(lines, g.grouped_elements[client_id])
}

// ---------------------------------
// MESSAGE FORMAT: OVERVIEW|BUDGET|REVENUE
// ---------------------------------
const OVERVIEW = 0
const BUDGET = 1
const REVENUE = 2

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
		current.budget += budget
		current.revenue += revenue
		current.count += 1
		grouped_elements[parts[OVERVIEW]] = current

	}
}

func storeGroupedElements(results map[string]RevenueBudgetCount, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByOverviewAndAvg(config GroupByOverviewAndAvgConfig, messages_before_commit int, eof_counter int) *GroupByOverviewAndAvg {
	log.Infof("GroupByOverviewAndAvg: %+v", config)
	return &GroupByOverviewAndAvg{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		expected_eof:           eof_counter,
		eofs:                   make(map[string]int),
		grouped_elements:       make(map[string]map[string]RevenueBudgetCount),
	}
}

func (g *GroupByOverviewAndAvg) RunWorker(starting_message string) error {
	msgs, err := common_group_by.Init(&g.Worker, starting_message)
	if err != nil {
		return err
	}
	return common_group_by.RunWorker(g, msgs)
}
