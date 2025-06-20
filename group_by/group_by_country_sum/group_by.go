package group_by_country_sum

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type GroupByCountryAndSumConfig struct {
	worker.WorkerConfig
}

type GroupByCountryAndSum struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]int
	eofs                   map[string]int
	storage_base_dir       string
	messages_id            map[string][]string
	messages               map[string][]amqp091.Delivery
}

var log = logging.MustGetLogger("group_by_country_sum")

func (g *GroupByCountryAndSum) EnsureClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]int)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *GroupByCountryAndSum) HandleCommit(client_id string, message amqp091.Delivery) {
	g.messages[client_id] = append(g.messages[client_id], message)
	if len(g.messages[client_id]) >= g.messages_before_commit {
		common_statefull_worker.StoreElementsWithMessageIds(g.grouped_elements[client_id],
			client_id, g.storage_base_dir,
			g.messages_id[client_id][len(g.messages_id[client_id])-g.messages_before_commit:])

		for _, message := range g.messages[client_id] {
			message.Ack(false)
		}
		g.messages[client_id] = g.messages[client_id][:0]
	}
}

func (g *GroupByCountryAndSum) MapToLines(client_id string) string {
	return mapToLines(g.grouped_elements[client_id])
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
	// g.eofs[client_id]++
	// if g.eofs[client_id] >= g.expected_eof {
	// 	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	delete(g.grouped_elements, client_id)
	// 	delete(g.eofs, client_id)
	// }
	for _, message := range g.messages[client_id] {
		message.Ack(false)
	}

	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	common_statefull_worker.StoreElementsWithMessageIds(g.grouped_elements[client_id], client_id, g.storage_base_dir, []string{message_id})

	delete(g.messages, client_id)
	delete(g.grouped_elements, client_id)
	delete(g.eofs, client_id)
	common_statefull_worker.CleanState(g.storage_base_dir, client_id)
	return nil
}

func (g *GroupByCountryAndSum) UpdateState(lines []string, client_id string, message_id string) {
	if slices.Contains(g.messages_id[client_id], message_id) {
		log.Warning("Mensaje repetido")
		return
	}
	g.messages_id[client_id] = append(g.messages_id[client_id], message_id)
	groupByCountryAndSum(lines, g.grouped_elements[client_id])
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
	log.Infof("GroupByCountryAndSum: %+v", config)
	grouped_elements, _, last_messages_in_state := common_statefull_worker.GetElements[int](storage_base_dir)
	messages_id, last_message_in_id := common_statefull_worker.GetIds(storage_base_dir)

	if common_statefull_worker.RestoreStateIfNeeded(last_messages_in_state, last_message_in_id, storage_base_dir) {
		messages_id, _ = common_statefull_worker.GetIds(storage_base_dir)
	}
	worker, err := worker.NewWorker(config.WorkerConfig, messages_before_commit)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &GroupByCountryAndSum{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		grouped_elements:       grouped_elements,
		eofs:                   make(map[string]int),
		storage_base_dir:       storage_base_dir,
		messages_id:            messages_id,
		messages:               make(map[string][]amqp091.Delivery),
	}
}
