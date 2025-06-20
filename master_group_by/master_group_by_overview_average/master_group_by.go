package master_group_by_overview_average

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/op/go-logging"
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
	node_name              string
	messages_id            map[string][]string
}

type ScoreAndCount struct {
	count int
	score float64
}

var log = logging.MustGetLogger("master_group_by_overview_average")

func (g *MasterGroupByOverviewAndAvg) NewClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]ScoreAndCount)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *MasterGroupByOverviewAndAvg) ShouldCommit(messages_before_commit int, client_id string, message_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElementsWithMovies(g.grouped_elements[client_id], client_id, g.node_name, message_id)
		return true
	}
	return false
}

func (g *MasterGroupByOverviewAndAvg) MapToLines(client_id string) string {
	return mapToLines(g.grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]ScoreAndCount) string {
	var lines []string
	for overview, value := range grouped_elements {
		average := value.score / float64(value.count)
		line := fmt.Sprintf("%s%s%f", overview, worker.MESSAGE_SEPARATOR, average)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *MasterGroupByOverviewAndAvg) HandleEOF(client_id string) error {
	log.Infof("MasterGroupByOverviewAndAvg: Handling EOF for client %s", client_id)
	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	delete(g.grouped_elements, client_id)
	delete(g.eofs, client_id)
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
		current.score += average
		current.count += count
		grouped_elements[parts[OVERVIEW]] = current

	}
}

func NewGroupByOverviewAndAvg(config MasterGroupByOverviewAndAvgConfig, messages_before_commit int, expected_eof int, node_name string) *MasterGroupByOverviewAndAvg {
	log.Infof("MasterGroupByOverviewAndAvg: %+v", config)

	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	grouped_elements, _, last_messages_in_state := common_statefull_worker.GetElements[ScoreAndCount](node_name)
	messages_id, last_messages_in_id := common_statefull_worker.GetIds(node_name)

	common_statefull_worker.RestoreStateIfNeeded(last_messages_in_state, last_messages_in_id, node_name)
	return &MasterGroupByOverviewAndAvg{
		Worker:                 *worker,
		messages_before_commit: messages_before_commit,
		expected_eof:           expected_eof,
		grouped_elements:       grouped_elements,
		eofs:                   make(map[string]int),
		node_name:              node_name,
		messages_id:            messages_id,
	}
}
