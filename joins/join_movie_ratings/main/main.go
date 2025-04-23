package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"

	join "distribuidos-tp1/joins/join_movie_ratings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("join_movie_ratings")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	log_level := v.GetString("log.level")
	inputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.input.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.input.routingkeys")},
		QueueName:   "join_movie_ratings_queue",
	}
	secondInputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.second_input.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.secondinput.routingkeys")},
		QueueName:   "join_movie_ratings_queue",
	}
	outputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.output.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.output.routingkeys")},
		QueueName:   "join_movie_ratings_queue",
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

	filter := join.NewJoinMovieRatingById(join.JoinMovieRatingByIdConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:       inputExchangeSpec,
			SecondInputExchange: secondInputExchangeSpec,
			OutputExchange:      outputExchangeSpec,
			MessageBroker:       messageBroker,
		},
	}, maxMessages)

	defer filter.CloseWorker()

	err = filter.RunWorker()
	if err != nil {
		panic(err)
	}
}
