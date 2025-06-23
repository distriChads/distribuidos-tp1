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

	log_level := v.GetString("cli.log.level")
	outputRoutingKeysJoinMovieRatings := strings.Split(v.GetString("ROUTINGKEYS_OUTPUT_JOIN-MOVIE-RATINGS"), ",")
	outputRoutingKeysJoinMovieCredits := strings.Split(v.GetString("ROUTINGKEYS_OUTPUT_JOIN-MOVIE-CREDITS"), ",")

	filterRoutingKeysMap := map[string][]string{
		"join_movie_ratings": outputRoutingKeysJoinMovieRatings,
		"join_movie_credits": outputRoutingKeysJoinMovieCredits,
	}

	exchangeSpec := worker.ExchangeSpec{
		InputRoutingKeys:  strings.Split(v.GetString("routingkeys.input"), ","),
		OutputRoutingKeys: filterRoutingKeysMap,
		QueueName:         "filter_after_2000",
	}
	messageBroker := v.GetString("cli.worker.broker")

	if exchangeSpec.InputRoutingKeys[0] == "" || len(exchangeSpec.OutputRoutingKeys) == 0 || messageBroker == "" {
		log.Criticalf("Error: one or more environment variables are empty --- message_broker: %s, input_routing_keys: %v, output_routing_keys: %v",
			messageBroker, exchangeSpec.InputRoutingKeys, filterRoutingKeysMap)
		return
	}

	if err := utils.InitLogger(log_level); err != nil {
		log.Criticalf("%s", err)
		return
	}

	eofCounter := v.GetInt("EOF_COUNTER")
	storage_base_dir := v.GetString("worker.storage")
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		join.RunWorker(ctx, "Starting join by movie credits")
		done <- true
	}()

	select {
	case sig := <-sigChan:
		log.Infof("Signal received: %s. Shutting down...", sig)
		cancel() // cancelamos el contexto → avisa al worker que debe salir
		<-done   // esperamos que termine
		join.CloseWorker()
		log.Info("Worker shut down successfully")
	case <-done:
		log.Info("Worker finished successfully")
	}

	wg.Wait()
}
