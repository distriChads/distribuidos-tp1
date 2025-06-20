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

	group_by "distribuidos-tp1/group_by/group_by_actor_count"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("group_by_actor_count")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	log_level := v.GetString("log.level")
	queue_name := strings.Split(v.GetString("worker.exchange.input.routingkeys"), ",")[0]

	exchangeSpec := worker.ExchangeSpec{
		InputRoutingKeys:  strings.Split(v.GetString("worker.exchange.input.routingkeys"), ","),
		OutputRoutingKeys: strings.Split(v.GetString("worker.exchange.output.routingkeys"), ","),
		QueueName:         queue_name,
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
	if maxMessages == 0 {
		maxMessages = 10
	}

	storage_base_dir := v.GetString("worker.storage")
	group_by := group_by.NewGroupByActorAndCount(group_by.GroupByActorAndCountConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	}, maxMessages, storage_base_dir)

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	done := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		common_statefull_worker.RunWorker(group_by, ctx, group_by.Worker, "Starting group by actor count")
		done <- true
	}()

	select {
	case sig := <-sigChan:
		log.Infof("Signal received: %s. Shutting down...", sig)
		cancel() // cancelamos el contexto → avisa al worker que debe salir
		<-done   // esperamos que termine
		group_by.CloseWorker()
		log.Info("Worker shut down successfully")
	case <-done:
		log.Info("Worker finished successfully")
	}

	wg.Wait()
}
