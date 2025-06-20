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

	master_group_by "distribuidos-tp1/master_group_by/master_group_by_country_sum"

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
	exchangeSpec := worker.ExchangeSpec{
		InputRoutingKeys:  strings.Split(v.GetString("worker.exchange.input.routingkeys"), ","),
		OutputRoutingKeys: strings.Split(v.GetString("worker.exchange.output.routingkeys"), ","),
		QueueName:         v.GetString("worker.queue.name"),
	}
	messageBroker := v.GetString("worker.broker")

	if exchangeSpec.InputRoutingKeys[0] == "" || exchangeSpec.OutputRoutingKeys[0] == "" || messageBroker == "" {
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

	storage_base_dir := v.GetString("worker.storage")
	master_group_by := master_group_by.NewGroupByCountryAndSum(master_group_by.MasterGroupByCountryAndSumConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	}, maxMessages, expectedEof, storage_base_dir)
	if master_group_by == nil {
		return
	}

	// Crear contexto cancelable
	ctx, cancel := context.WithCancel(context.Background())

	// Capturar señales del sistema
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Lanzar worker en goroutine
	done := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		common_statefull_worker.RunWorker(master_group_by, ctx, master_group_by.Worker, "starting master group by actor count")
		done <- true
	}()

	// Esperar señal o finalización
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
