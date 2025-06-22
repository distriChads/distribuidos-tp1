package group_by_country_sum

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strconv"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

type GroupByCountryAndSumConfig struct {
	worker.WorkerConfig
}

type GroupByCountryAndSum struct {
	*common_group_by.CommonGroupBy[int]
}

func (g *GroupByCountryAndSum) EnsureClient(client_id string) {
	g.CommonGroupBy.EnsureClient(client_id)
}

func (g *GroupByCountryAndSum) HandleCommit(client_id string, message amqp091.Delivery) error {
	return g.CommonGroupBy.HandleCommit(client_id, message)
}

func (g *GroupByCountryAndSum) MapToLines(client_id string) string {
	return mapToLines(g.CommonGroupBy.Grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]int) string {
	var lines []string
	for country, budget := range grouped_elements {
		line := fmt.Sprintf("%s%s%d", country, worker.MESSAGE_SEPARATOR, budget)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *GroupByCountryAndSum) HandleEOF(client_id string, message_id string) error {
	return g.CommonGroupBy.HandleEOF(client_id, message_id, g.MapToLines(client_id))

}

func (g *GroupByCountryAndSum) UpdateState(lines []string, client_id string, message_id string) {
	if !g.CommonGroupBy.VerifyRepeatedMessage(client_id, message_id) {
		groupByCountryAndSum(lines, g.CommonGroupBy.Grouped_elements[client_id])
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRY|GENRES|BUDGET
// ---------------------------------
const COUNTRY = 3
const BUDGET = 5

func groupByCountryAndSum(lines []string, grouped_elements map[string]int) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		budget, err := strconv.Atoi(parts[BUDGET])
		if err != nil {
			continue
		}
		grouped_elements[parts[COUNTRY]] += budget
	}
}

func NewGroupByCountryAndSum(config GroupByCountryAndSumConfig, messages_before_commit int, storage_base_dir string) *GroupByCountryAndSum {
	group_by := common_group_by.NewCommonGroupBy[int](config.WorkerConfig, messages_before_commit, storage_base_dir)
	return &GroupByCountryAndSum{
		CommonGroupBy: group_by,
	}
}
