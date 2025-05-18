package master_group_by_overview_average

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
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

func (g *MasterGroupByOverviewAndAvg) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		storeGroupedElements(g.grouped_elements[client_id], client_id)
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
	g.eofs[client_id]++
	if g.eofs[client_id] >= g.expected_eof {
		err := common_statefull_worker.SendResult(g.Worker, g, client_id)
		if err != nil {
			return err
		}
		delete(g.grouped_elements, client_id)
		delete(g.eofs, client_id)
	}
	return nil
}

func (g *MasterGroupByOverviewAndAvg) UpdateState(lines []string, client_id string) {
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

func storeGroupedElements(results map[string]ScoreAndCount, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() map[string]float64 {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByOverviewAndAvg(config MasterGroupByOverviewAndAvgConfig, messages_before_commit int, expected_eof int) *MasterGroupByOverviewAndAvg {
	log.Infof("MasterGroupByOverviewAndAvg: %+v", config)
	return &MasterGroupByOverviewAndAvg{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		expected_eof:           expected_eof,
		grouped_elements:       make(map[string]map[string]ScoreAndCount),
		eofs:                   make(map[string]int),
	}
}
func (g *MasterGroupByOverviewAndAvg) RunWorker(starting_message string) error {
	msgs, err := common_statefull_worker.Init(&g.Worker, starting_message)
	if err != nil {
		return err
	}
	return common_statefull_worker.RunWorker(g, msgs)
}
