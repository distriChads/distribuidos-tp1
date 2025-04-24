package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"

	group_by "distribuidos-tp1/group_by/group_by_country_sum"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("group_by_country_sum")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	log_level := v.GetString("log.level")
	inputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.input.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.input.routingKeys")},
		QueueName:   "group_by_country_sum_queue",
	}
	outputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.output.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.output.routingKeys")},
		QueueName:   "group_by_country_sum_queue",
	}
	messageBroker := v.GetString("worker.broker")

	if inputExchangeSpec.Name == "" || inputExchangeSpec.RoutingKeys[0] == "" || outputExchangeSpec.Name == "" || outputExchangeSpec.RoutingKeys[0] == "" || messageBroker == "" {
		log.Criticalf("Error: one or more environment variables are empty")
		return
	}

	if err := utils.InitLogger(log_level); err != nil {
		log.Criticalf("%s", err)
		return
	}

	maxMessages := v.GetInt("worker.maxmessages")
	if maxMessages == 0 {
		maxMessages = 10
	}
	expectedEof := v.GetInt("worker.expectedeof")
	if expectedEof == 0 {
		expectedEof = 1
	}

	groupByCountrySum := group_by.NewGroupByCountryAndSum(group_by.GroupByCountryAndSumConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  inputExchangeSpec,
			OutputExchange: outputExchangeSpec,
			MessageBroker:  messageBroker,
		},
	}, maxMessages, expectedEof)

	defer groupByCountrySum.CloseWorker()

	err = groupByCountrySum.RunWorker()
	if err != nil {
		panic(err)
	}
}
