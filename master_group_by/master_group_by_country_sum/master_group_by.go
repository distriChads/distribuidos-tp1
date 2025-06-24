package master_group_by_country_sum

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strconv"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

type MasterGroupByCountryAndSumConfig struct {
	worker.WorkerConfig
}

type MasterGroupByCountryAndSum struct {
	*common_group_by.CommonGroupBy[int]
}

func (g *MasterGroupByCountryAndSum) EnsureClient(client_id string) {
	g.CommonGroupBy.EnsureClient(client_id)
}

func (g *MasterGroupByCountryAndSum) HandleCommit(client_id string, message amqp091.Delivery) error {
	return g.CommonGroupBy.HandleCommit(client_id, message)
}

func (g *MasterGroupByCountryAndSum) MapToLines(client_id string) string {
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

func (g *MasterGroupByCountryAndSum) HandleEOF(client_id string, message_id string) error {
	return g.CommonGroupBy.HandleEOF(client_id, message_id, g.MapToLines(client_id))
}

func (g *MasterGroupByCountryAndSum) UpdateState(lines []string, client_id string, message_id string) bool {
	repeated_message := g.CommonGroupBy.VerifyRepeatedMessage(client_id, message_id)
	if !repeated_message {
		groupByCountryAndSum(lines, g.CommonGroupBy.Grouped_elements[client_id])
	}
	return repeated_message
}

// ---------------------------------
// MESSAGE FORMAT: COUNTRY|BUDGET
// ---------------------------------
const COUNTRY = 0
const BUDGET = 1

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

func NewGroupByCountryAndSum(config MasterGroupByCountryAndSumConfig, messages_before_commit int, expected_eof int, storage_base_dir string) *MasterGroupByCountryAndSum {
	group_by := common_group_by.NewCommonGroupBy[int](config.WorkerConfig, messages_before_commit, storage_base_dir, expected_eof)
	return &MasterGroupByCountryAndSum{
		CommonGroupBy: group_by,
	}

}
