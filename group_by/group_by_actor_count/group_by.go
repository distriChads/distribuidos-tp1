package group_by_actor_count

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

type GroupByActorAndCountConfig struct {
	worker.WorkerConfig
}

type GroupByActorAndCount struct {
	*common_group_by.CommonGroupBy[int]
}

func (g *GroupByActorAndCount) EnsureClient(client_id string) {
	g.CommonGroupBy.EnsureClient(client_id)
}

func (g *GroupByActorAndCount) HandleCommit(client_id string, message amqp091.Delivery) error {
	return g.CommonGroupBy.HandleCommit(client_id, message)
}

func (g *GroupByActorAndCount) MapToLines(client_id string) string {
	return mapToLines(g.CommonGroupBy.Grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]int) string {
	var lines []string
	for actor, count := range grouped_elements {
		line := fmt.Sprintf("%s%s%d", actor, worker.MESSAGE_SEPARATOR, count)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *GroupByActorAndCount) HandleEOF(client_id string, message_id string) error {
	return g.CommonGroupBy.HandleEOF(client_id, message_id, g.MapToLines(client_id))
}

func (g *GroupByActorAndCount) UpdateState(lines []string, client_id string, message_id string) {
	if !g.CommonGroupBy.VerifyRepeatedMessage(client_id, message_id) {
		groupByActorAndUpdate(lines, g.CommonGroupBy.Grouped_elements[client_id])
	}
}

const ACTORS = 1

func groupByActorAndUpdate(lines []string, grouped_elements map[string]int) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)

		actors := strings.Split(parts[ACTORS], worker.MESSAGE_ARRAY_SEPARATOR)
		for _, actor := range actors {
			grouped_elements[actor] += 1
		}
	}
}

func NewGroupByActorAndCount(config GroupByActorAndCountConfig, messages_before_commit int, storage_base_dir string) *GroupByActorAndCount {
	group_by := common_group_by.NewCommonGroupBy[int](config.WorkerConfig, messages_before_commit, storage_base_dir)
	return &GroupByActorAndCount{
		CommonGroupBy: group_by,
	}
}
