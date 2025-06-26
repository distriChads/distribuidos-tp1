package main

import (
	"context"
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"distribuidos-tp1/filters/common_stateless_worker"
	filter "distribuidos-tp1/filters/filter_after_2000"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_after_2000")

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

	filter := filter.NewFilterByAfterYear2000(filter.FilterByAfterYear2000Config{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	})
	if filter == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	done := make(chan bool)
	wg := sync.WaitGroup{}

	heartbeat_port := v.GetInt("cli.heartbeat.port")
	wg.Add(1)
	go func() {
		defer wg.Done()
		utils.HeartBeat(ctx, heartbeat_port)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		common_stateless_worker.RunWorker(filter, *filter.Worker, ctx, "Starting filter by after 2000")
		done <- true
	}()

	select {
	case sig := <-sigChan:
		log.Infof("Signal received: %s. Initiating shutdown...", sig)
		cancel()
		<-done
		filter.CloseWorker()
		log.Info("Worker shut down successfully")
	case <-done:
		log.Info("Worker finished successfully")
	}

	wg.Wait()
}
