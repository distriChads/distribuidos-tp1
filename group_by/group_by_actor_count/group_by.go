package group_by_actor_count

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"strings"

	"github.com/op/go-logging"
)

type GroupByActorAndCountConfig struct {
	worker.WorkerConfig
}

type GroupByActorAndCount struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]int
	eofs                   map[string]int
	node_name              string
	log_replicas           int
}

var log = logging.MustGetLogger("group_by_actor_count")

func (g *GroupByActorAndCount) NewClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]int)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *GroupByActorAndCount) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElements(g.grouped_elements[client_id], client_id, g.node_name, g.log_replicas)
		return true
	}
	return false
}

func (g *GroupByActorAndCount) MapToLines(client_id string) string {
	return mapToLines(g.grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]int) string {
	var lines []string
	for actor, count := range grouped_elements {
		line := fmt.Sprintf("%s%s%d", actor, worker.MESSAGE_SEPARATOR, count)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *GroupByActorAndCount) HandleEOF(client_id string) error {
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

func (g *GroupByActorAndCount) UpdateState(lines []string, client_id string) {
	groupByActorAndUpdate(lines, g.grouped_elements[client_id])
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

func NewGroupByActorAndCount(config GroupByActorAndCountConfig, messages_before_commit int, node_name string) *GroupByActorAndCount {
	log.Infof("GroupByActorAndCount: %+v", config)
	replicas := 3
	grouped_elements, _ := common_statefull_worker.GetElements[int](node_name, replicas+1)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}
	return &GroupByActorAndCount{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		grouped_elements:       grouped_elements,
		eofs:                   make(map[string]int),
		node_name:              node_name,
		log_replicas:           3,
	}
}
