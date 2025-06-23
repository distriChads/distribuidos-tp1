package group_by_overview_average

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strconv"
	"strings"

	"github.com/rabbitmq/amqp091-go"
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
	*common_group_by.CommonGroupBy[RevenueBudgetCount]
}

type RevenueBudgetCount struct {
	Count               int
	Sum_revenue_average float64
}

func (g *GroupByOverviewAndAvg) EnsureClient(client_id string) {
	g.CommonGroupBy.EnsureClient(client_id)
}

func (g *GroupByOverviewAndAvg) HandleCommit(client_id string, message amqp091.Delivery) error {
	return g.CommonGroupBy.HandleCommit(client_id, message)
}

func (g *GroupByOverviewAndAvg) MapToLines(client_id string) string {
	return mapToLines(g.CommonGroupBy.Grouped_elements[client_id])
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

func (g *GroupByOverviewAndAvg) HandleEOF(client_id string, message_id string) error {
	return g.CommonGroupBy.HandleEOF(client_id, message_id, g.MapToLines(client_id))
}

func (g *GroupByOverviewAndAvg) UpdateState(lines []string, client_id string, message_id string) bool {
	repeated_message := g.CommonGroupBy.VerifyRepeatedMessage(client_id, message_id)
	if !repeated_message {
		groupByOverviewAndUpdate(lines, g.CommonGroupBy.Grouped_elements[client_id])
	}
	return repeated_message
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

func NewGroupByOverviewAndAvg(config GroupByOverviewAndAvgConfig, messages_before_commit int, storage_base_dir string, expected_eof int) *GroupByOverviewAndAvg {
	group_by := common_group_by.NewCommonGroupBy[RevenueBudgetCount](config.WorkerConfig, messages_before_commit, storage_base_dir, expected_eof)
	return &GroupByOverviewAndAvg{
		CommonGroupBy: group_by,
	}
}
