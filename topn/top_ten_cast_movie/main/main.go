package main

import (
	"context"
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	topn "distribuidos-tp1/topn/top_ten_cast_movie"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("top_ten_cast_movie")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	log_level := v.GetString("cli.log.level")
	outputRoutingKeysResults := strings.Split(v.GetString("ROUTINGKEYS_OUTPUT_OUTPUT_ROUTINGKEY"), ",")

	filterRoutingKeysMap := map[string][]string{
		"results": outputRoutingKeysResults,
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

	storage_base_dir := v.GetString("cli.worker.storage")
	topn := topn.NewTopTenCastMovie(topn.TopTenCastMovieConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	}, storage_base_dir)
	if topn == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

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
		common_statefull_worker.RunWorker(topn, ctx, topn.Worker, "starting first and last")
		done <- true
	}()

	select {
	case sig := <-sigChan:
		log.Infof("Signal received: %s. Shutting down...", sig)
		cancel()
		<-done
		topn.CloseWorker()
		log.Info("Worker shut down successfully")
	case <-done:
		log.Info("Worker finished successfully")
	}

	wg.Wait()
}
