package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"strings"

	join "distribuidos-tp1/joins/join_movie_credits"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("join_movie_credits")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	log_level := v.GetString("log.level")

	println(("first exchange name: " + v.GetString("worker.exchange.input.name")))
	println(("second exchange name: " + v.GetString("worker.exchange.secondinput.name")))

	queueName := v.GetString("worker.queue.name")
	inputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.input.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.input.routingkeys")},
		QueueName:   queueName,
	}
	secondInputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.secondinput.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.secondinput.routingkeys")},
		QueueName:   queueName + "2",
	}
	outputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.output.name"),
		RoutingKeys: strings.Split(v.GetString("worker.exchange.output.routingkeys"), ","),
		QueueName:   queueName,
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

	filter := join.NewJoinMovieCreditsById(join.JoinMovieCreditsByIdConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:       inputExchangeSpec,
			SecondInputExchange: secondInputExchangeSpec,
			OutputExchange:      outputExchangeSpec,
			MessageBroker:       messageBroker,
		},
	}, maxMessages, expectedEof)

	defer filter.CloseWorker()

	err = filter.RunWorker()
	if err != nil {
		panic(err)
	}
}
