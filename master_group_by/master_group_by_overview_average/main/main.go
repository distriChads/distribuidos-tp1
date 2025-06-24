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

	master_group_by "distribuidos-tp1/master_group_by/master_group_by_overview_average"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("master_group_by_overview_average")

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

	maxMessages := v.GetInt("cli.worker.maxmessages")
	expectedEof := v.GetInt("EOF_COUNTER")
	storage_base_dir := v.GetString("cli.worker.storage")

	master_group_by := master_group_by.NewGroupByOverviewAndAvg(master_group_by.MasterGroupByOverviewAndAvgConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	}, maxMessages, expectedEof, storage_base_dir)
	if master_group_by == nil {
		return
	}

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
		common_statefull_worker.RunWorker(master_group_by, ctx, master_group_by.Worker, "starting master group by actor count")
		done <- true
	}()

	select {
	case sig := <-sigChan:
		log.Infof("Signal received: %s. Shutting down...", sig)
		cancel()                      // Graceful shutdown
		<-done                        // Esperamos que termine el worker
		master_group_by.CloseWorker() // Limpiamos recursos
		log.Info("Worker shut down successfully")
	case <-done:
		log.Info("Worker finished successfully")
	}

	wg.Wait()
}
