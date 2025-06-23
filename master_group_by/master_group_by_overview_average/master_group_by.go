package master_group_by_overview_average

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strconv"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

type MasterGroupByOverviewAndAvgConfig struct {
	worker.WorkerConfig
}

type MasterGroupByOverviewAndAvg struct {
	*common_group_by.CommonGroupBy[ScoreAndCount]
}

type ScoreAndCount struct {
	Count int
	Score float64
}

func (g *MasterGroupByOverviewAndAvg) EnsureClient(client_id string) {
	g.CommonGroupBy.EnsureClient(client_id)
}

func (g *MasterGroupByOverviewAndAvg) HandleCommit(client_id string, message amqp091.Delivery) error {
	return g.CommonGroupBy.HandleCommit(client_id, message)
}

func (g *MasterGroupByOverviewAndAvg) MapToLines(client_id string) string {
	return mapToLines(g.CommonGroupBy.Grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]ScoreAndCount) string {
	var lines []string
	for overview, value := range grouped_elements {
		average := value.Score / float64(value.Count)
		line := fmt.Sprintf("%s%s%f", overview, worker.MESSAGE_SEPARATOR, average)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *MasterGroupByOverviewAndAvg) HandleEOF(client_id string, message_id string) error {
	return g.CommonGroupBy.HandleEOF(client_id, message_id, g.MapToLines(client_id))
}

func (g *MasterGroupByOverviewAndAvg) UpdateState(lines []string, client_id string, message_id string) {
	if !g.CommonGroupBy.VerifyRepeatedMessage(client_id, message_id) {
		groupByOverviewAndUpdate(lines, g.CommonGroupBy.Grouped_elements[client_id])
	}
}

// ---------------------------------
// MESSAGE FORMAT: OVERVIEW|AVERAGE
// ---------------------------------
const OVERVIEW = 0
const AVERAGE = 1
const COUNT = 2

func groupByOverviewAndUpdate(lines []string, grouped_elements map[string]ScoreAndCount) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		average, err := strconv.ParseFloat(parts[AVERAGE], 64)
		if err != nil {
			continue
		}
		count, err := strconv.Atoi(parts[COUNT])
		if err != nil {
			continue
		}

		current := grouped_elements[parts[OVERVIEW]]
		current.Score += average
		current.Count += count
		grouped_elements[parts[OVERVIEW]] = current

	}
}

func NewGroupByOverviewAndAvg(config MasterGroupByOverviewAndAvgConfig, messages_before_commit int, expected_eof int, storage_base_dir string) *MasterGroupByOverviewAndAvg {
	group_by := common_group_by.NewCommonGroupBy[ScoreAndCount](config.WorkerConfig, messages_before_commit, storage_base_dir, expected_eof)
	return &MasterGroupByOverviewAndAvg{
		CommonGroupBy: group_by,
	}
}
