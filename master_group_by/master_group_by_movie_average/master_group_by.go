package master_group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type MasterGroupByMovieAndAvgConfig struct {
	worker.WorkerConfig
}

var log = logging.MustGetLogger("master_group_by_movie_average")

type MasterGroupByMovieAndAvg struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]float64
	eofs                   map[string]int
}

func (g *MasterGroupByMovieAndAvg) NewClient(client_id string) {
	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]float64)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *MasterGroupByMovieAndAvg) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		storeGroupedElements(g.grouped_elements[client_id], client_id)
		return true
	}
	return false
}

func (g *MasterGroupByMovieAndAvg) MapToLines(client_id string) string {
	return mapToLines(g.grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]float64) string {
	var lines []string
	for movie, average := range grouped_elements {
		line := fmt.Sprintf("%s%s%f", movie, worker.MESSAGE_SEPARATOR, average)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *MasterGroupByMovieAndAvg) HandleEOF(client_id string) error {
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

func (g *MasterGroupByMovieAndAvg) GroupByAndUpdate(lines []string, client_id string) {
	groupByMovieAndUpdate(lines, g.grouped_elements[client_id])
}

// ---------------------------------
// MESSAGE FORMAT: TITLE|AVERAGE
// ---------------------------------
const TITLE = 0
const SCORE = 1

func groupByMovieAndUpdate(lines []string, grouped_elements map[string]float64) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		if len(parts) < 2 {
			log.Errorf("Invalid message format: %s", line)
		}
		average, err := strconv.ParseFloat(parts[SCORE], 64)
		if err != nil {
			continue
		}

		grouped_elements[parts[TITLE]] += average

	}
}

func storeGroupedElements(results map[string]float64, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() map[string]int {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return nil
}

func NewGroupByMovieAndAvg(config MasterGroupByMovieAndAvgConfig, messages_before_commit int, expected_eof int) *MasterGroupByMovieAndAvg {
	log.Infof("MasterGroupByMovieAndAvg: %+v", config)
	return &MasterGroupByMovieAndAvg{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		expected_eof:           expected_eof,
		grouped_elements:       make(map[string]map[string]float64),
		eofs:                   make(map[string]int),
	}
}

func (g *MasterGroupByMovieAndAvg) RunWorker(starting_message string) error {
	msgs, err := common_group_by.Init(&g.Worker, starting_message)
	if err != nil {
		return err
	}
	return common_group_by.RunWorker(g, msgs)
}
