package master_group_by_overview_average

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

type MasterGroupByOverviewAndAvgConfig struct {
	worker.WorkerConfig
}

type MasterGroupByOverviewAndAvg struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]ScoreAndCount
	eofs                   map[string]int
	storage_base_dir       string
	messages_id            map[string][]string
	messages               map[string][]amqp091.Delivery
}

type ScoreAndCount struct {
	Count int
	Score float64
}

var log = logging.MustGetLogger("master_group_by_overview_average")

func (g *MasterGroupByOverviewAndAvg) EnsureClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]ScoreAndCount)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *MasterGroupByOverviewAndAvg) HandleCommit(client_id string, message amqp091.Delivery) {
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

func (g *MasterGroupByOverviewAndAvg) MapToLines(client_id string) string {
	return mapToLines(g.grouped_elements[client_id])
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

	for _, message := range g.messages[client_id] {
		message.Ack(false)
	}

	log.Infof("MasterGroupByOverviewAndAvg: Handling EOF for client %s", client_id)
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

func (g *MasterGroupByOverviewAndAvg) UpdateState(lines []string, client_id string, message_id string) {
	if slices.Contains(g.messages_id[client_id], message_id) {
		log.Warning("Mensaje repetido")
		return
	}
	g.messages_id[client_id] = append(g.messages_id[client_id], message_id)
	groupByOverviewAndUpdate(lines, g.grouped_elements[client_id])
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
	log.Infof("MasterGroupByOverviewAndAvg: %+v", config)

	worker, err := worker.NewWorker(config.WorkerConfig, 10)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	grouped_elements, _, last_messages_in_state := common_statefull_worker.GetElements[ScoreAndCount](storage_base_dir)
	messages_id, last_message_in_id := common_statefull_worker.GetIds(storage_base_dir)

	if common_statefull_worker.RestoreStateIfNeeded(last_messages_in_state, last_message_in_id, storage_base_dir) {
		messages_id, _ = common_statefull_worker.GetIds(storage_base_dir)
	}
	return &MasterGroupByOverviewAndAvg{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		expected_eof:           expected_eof,
		grouped_elements:       grouped_elements,
		eofs:                   make(map[string]int),
		storage_base_dir:       storage_base_dir,
		messages_id:            messages_id,
		messages:               make(map[string][]amqp091.Delivery),
	}
}
