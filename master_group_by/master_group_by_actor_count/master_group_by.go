package master_group_by_actor_count

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
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

func (g *MasterGroupByActorAndCount) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		storeGroupedElements(g.grouped_elements[client_id], client_id)
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

func (g *MasterGroupByActorAndCount) UpdateState(lines []string, client_id string) {
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

func storeGroupedElements(results map[string]int, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByActorAndCount(config MasterGroupByActorAndCountConfig, messages_before_commit int, expected_eof int) *MasterGroupByActorAndCount {
	log.Infof("MasterGroupByActorAndCount: %+v", config)
	return &MasterGroupByActorAndCount{
		Worker: worker.Worker{
			Exchange:      config.Exchange,
			MessageBroker: config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		grouped_elements:       make(map[string]map[string]int),
		eofs:                   make(map[string]int),
	}
}

func (g *MasterGroupByActorAndCount) RunWorker(starting_message string) error {
	msgs, err := common_statefull_worker.Init(&g.Worker, starting_message)
	if err != nil {
		return err
	}
	return common_statefull_worker.RunWorker(g, msgs)
}
