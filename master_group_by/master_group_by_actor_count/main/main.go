package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"

	master_group_by "distribuidos-tp1/master_group_by/master_group_by_actor_count"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("master_group_by_actor_count")

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
		QueueName:   "master_group_by_actor_count_queue",
	}
	outputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.output.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.output.routingKeys")},
		QueueName:   "master_group_by_actor_count_queue",
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
	expectedEof := v.GetInt("worker.expectedeof")
	if maxMessages == 0 {
		maxMessages = 10
	}
	if expectedEof == 0 {
		expectedEof = 1
	}

	masterGroupByActorCount := master_group_by.NewGroupByActorAndCount(master_group_by.MasterGroupByActorAndCountConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  inputExchangeSpec,
			OutputExchange: outputExchangeSpec,
			MessageBroker:  messageBroker,
		},
	}, maxMessages, expectedEof)

	defer masterGroupByActorCount.CloseWorker()

	err = masterGroupByActorCount.RunWorker()
	if err != nil {
		panic(err)
	}
}
