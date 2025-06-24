package main

import (
	"context"
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/joins/join_movie_credits"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("join_movie_credits")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	messageBroker := v.GetString("cli.worker.broker")
	log_level := v.GetString("cli.log.level")
	eofCounter := v.GetInt("EOF_COUNTER")
	inputRoutingKeys := strings.Split(v.GetString("routingkeys.input"), ",")
	outputRoutingKeysJoinMovieRatings := strings.Split(v.GetString("ROUTINGKEYS_OUTPUT_GROUP-BY-ACTOR-COUNT"), ",")
	storage_base_dir := v.GetString("cli.worker.storage")

	log.Infof("Input routing keys: %v", inputRoutingKeys)

	outputRoutingKey := map[string][]string{
		"group_by_actor_count": outputRoutingKeysJoinMovieRatings,
	}

	exchangeSpec := worker.ExchangeSpec{
		InputRoutingKeys:  inputRoutingKeys,
		OutputRoutingKeys: outputRoutingKey,
		QueueName:         "filter_after_2000",
	}

	if exchangeSpec.InputRoutingKeys[0] == "" || len(exchangeSpec.OutputRoutingKeys) == 0 || messageBroker == "" {
		log.Criticalf("Error: one or more environment variables are empty --- message_broker: %s, input_routing_keys: %v, output_routing_keys: %v",
			messageBroker, exchangeSpec.InputRoutingKeys, outputRoutingKey)
		return
	}

	if err := utils.InitLogger(log_level); err != nil {
		log.Criticalf("%s", err)
		return
	}

	join := join_movie_credits.NewJoinMovieCreditsById(join_movie_credits.JoinMovieCreditsByIdConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	}, storage_base_dir, eofCounter)

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	done := make(chan bool)
	wg := sync.WaitGroup{}

	heartbeat_port := v.GetInt("heartbeat.port")
	wg.Add(1)
	go func() {
		defer wg.Done()
		utils.HeartBeat(ctx, heartbeat_port)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		join.RunWorker(ctx, "Starting join by movie credits")
		done <- true
	}()

	select {
	case sig := <-sigChan:
		log.Infof("Signal received: %s. Shutting down...", sig)
		cancel()
		<-done
		join.CloseWorker()
		log.Info("Worker shut down successfully")
	case <-done:
		log.Info("Worker finished successfully")
	}

	wg.Wait()
}
