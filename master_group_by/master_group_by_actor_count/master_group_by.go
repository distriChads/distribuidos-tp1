package master_group_by_actor_count

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strconv"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

type MasterGroupByActorAndCountConfig struct {
	worker.WorkerConfig
}

type MasterGroupByActorAndCount struct {
	*common_group_by.CommonGroupBy[int]
}

func (g *MasterGroupByActorAndCount) EnsureClient(client_id string) {
	g.CommonGroupBy.EnsureClient(client_id)
}

func (g *MasterGroupByActorAndCount) HandleCommit(client_id string, message amqp091.Delivery) error {
	return g.CommonGroupBy.HandleCommit(client_id, message)
}

func (g *MasterGroupByActorAndCount) MapToLines(client_id string) string {
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

func (g *MasterGroupByActorAndCount) HandleEOF(client_id string, message_id string) error {
	return g.CommonGroupBy.HandleEOF(client_id, message_id, g.MapToLines(client_id))
}

func (g *MasterGroupByActorAndCount) UpdateState(lines []string, client_id string, message_id string) bool {
	repeated_message := g.CommonGroupBy.VerifyRepeatedMessage(client_id, message_id)
	if !repeated_message {
		groupByActorAndUpdate(lines, g.CommonGroupBy.Grouped_elements[client_id])
	}
	return repeated_message
}

// ---------------------------------
// MESSAGE FORMAT: ACTOR|COUNT
// ---------------------------------
const ACTOR = 0
const COUNT = 1

// Store the actor as a key and sum the counter by the amount received
func groupByActorAndUpdate(lines []string, grouped_elements map[string]int) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)

		count, err := strconv.Atoi(parts[COUNT])
		if err != nil {
			continue
		}
		grouped_elements[parts[ACTOR]] += count
	}
}

func NewGroupByActorAndCount(config MasterGroupByActorAndCountConfig, expected_eof int, storage_base_dir string) *MasterGroupByActorAndCount {
	group_by := common_group_by.NewCommonGroupBy[int](config.WorkerConfig, 1, storage_base_dir, expected_eof)
	return &MasterGroupByActorAndCount{
		CommonGroupBy: group_by,
	}
}
