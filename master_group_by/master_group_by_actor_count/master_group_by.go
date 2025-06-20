package master_group_by_actor_count

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type MasterGroupByActorAndCountConfig struct {
	worker.WorkerConfig
}

type MasterGroupByActorAndCount struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]int
	eofs                   map[string]int
	node_name              string
	messages_id            map[string][]string
}

var log = logging.MustGetLogger("master_group_by_actor_count")

func (g *MasterGroupByActorAndCount) NewClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]int)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *MasterGroupByActorAndCount) ShouldCommit(messages_before_commit int, client_id string, message_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElementsWithMovies(g.grouped_elements[client_id], client_id, g.node_name, message_id)
		return true
	}
	return false
}

func (g *MasterGroupByActorAndCount) MapToLines(client_id string) string {
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

func (g *MasterGroupByActorAndCount) HandleEOF(client_id string) error {
	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	delete(g.grouped_elements, client_id)
	delete(g.eofs, client_id)
	return nil
}

func (g *MasterGroupByActorAndCount) UpdateState(lines []string, client_id string, message_id string) {
	if slices.Contains(g.messages_id[client_id], message_id) {
		log.Warning("Mensaje repetido")
		return
	}
	g.messages_id[client_id] = append(g.messages_id[client_id], message_id)
	groupByActorAndUpdate(lines, g.grouped_elements[client_id])
}

// ---------------------------------
// MESSAGE FORMAT: ACTOR|COUNT
// ---------------------------------
const ACTOR = 0
const COUNT = 1

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

func NewGroupByActorAndCount(config MasterGroupByActorAndCountConfig, messages_before_commit int, expected_eof int, node_name string) *MasterGroupByActorAndCount {
	log.Infof("MasterGroupByActorAndCount: %+v", config)

	grouped_elements, _, last_messages_in_state := common_statefull_worker.GetElements[int](node_name)
	messages_id, last_messages_in_id := common_statefull_worker.GetIds(node_name)

	common_statefull_worker.RestoreStateIfNeeded(last_messages_in_state, last_messages_in_id, node_name)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &MasterGroupByActorAndCount{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		grouped_elements:       grouped_elements,
		eofs:                   make(map[string]int),
		node_name:              node_name,
		messages_id:            messages_id,
	}
}
