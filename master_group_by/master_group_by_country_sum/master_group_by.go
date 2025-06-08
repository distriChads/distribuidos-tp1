package master_group_by_country_sum

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type MasterGroupByCountryAndSumConfig struct {
	worker.WorkerConfig
}

type MasterGroupByCountryAndSum struct {
	worker.Worker
	messages_before_commit int
	expected_eof           int
	grouped_elements       map[string]map[string]int
	eofs                   map[string]int
	node_name              string
	log_replicas           int
}

var log = logging.MustGetLogger("master_group_by_country_sum")

func (g *MasterGroupByCountryAndSum) NewClient(client_id string) {

	if _, ok := g.grouped_elements[client_id]; !ok {
		g.grouped_elements[client_id] = make(map[string]int)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = 0
	}
}

func (g *MasterGroupByCountryAndSum) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		common_statefull_worker.StoreElements(g.grouped_elements[client_id], client_id, g.node_name, g.log_replicas)
		return true
	}
	return false
}

func (g *MasterGroupByCountryAndSum) MapToLines(client_id string) string {
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

func (g *MasterGroupByCountryAndSum) HandleEOF(client_id string) error {
	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	delete(g.grouped_elements, client_id)
	delete(g.eofs, client_id)
	return nil
}

func (g *MasterGroupByCountryAndSum) UpdateState(lines []string, client_id string) {
	groupByCountryAndSum(lines, g.grouped_elements[client_id])
}

// ---------------------------------
// MESSAGE FORMAT: COUNTRY|BUDGET
// ---------------------------------
const COUNTRY = 0
const BUDGET = 1

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

func NewGroupByCountryAndSum(config MasterGroupByCountryAndSumConfig, messages_before_commit int, expected_eof int, node_name string) *MasterGroupByCountryAndSum {
	log.Infof("MasterGroupByCountryAndSum: %+v", config)
	replicas := 3
	grouped_elements, _ := common_statefull_worker.GetElements[int](node_name, replicas+1)
	return &MasterGroupByCountryAndSum{
		Worker: worker.Worker{
			Exchange:      config.Exchange,
			MessageBroker: config.MessageBroker,
		},
		messages_before_commit: messages_before_commit,
		grouped_elements:       grouped_elements,
		eofs:                   make(map[string]int),
		log_replicas:           replicas,
	}
}
