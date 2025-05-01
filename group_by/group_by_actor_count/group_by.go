package group_by_actor_count

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
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
		storeGroupedElements(g.grouped_elements[client_id], client_id)
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

func (g *GroupByActorAndCount) GroupByAndUpdate(lines []string, client_id string) {
	groupByActorAndUpdate(lines, g.grouped_elements[client_id])
}

func groupByActorAndUpdate(lines []string, grouped_elements map[string]int) {
	for _, line := range lines {
		actors := strings.Split(line, worker.MESSAGE_ARRAY_SEPARATOR)
		for _, actor := range actors {
			grouped_elements[actor] += 1
		}
	}
}

func storeGroupedElements(results map[string]int, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByActorAndCount(config GroupByActorAndCountConfig, messages_before_commit int, eof_counter int) *GroupByActorAndCount {
	log.Infof("GroupByActorAndCount: %+v", config)
	return &GroupByActorAndCount{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		expected_eof:           eof_counter,
		grouped_elements:       make(map[string]map[string]int),
		eofs:                   make(map[string]int),
	}
}

func (g *GroupByActorAndCount) RunWorker(starting_message string) error {
	msgs, err := common_group_by.Init(&g.Worker, starting_message)
	if err != nil {
		return err
	}
	return common_group_by.RunWorker(g, msgs)
}
