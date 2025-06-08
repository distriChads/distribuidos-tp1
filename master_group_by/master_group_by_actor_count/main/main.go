package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

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

	node_name := v.GetString("worker.nodename")
	master_group_by := master_group_by.NewGroupByActorAndCount(master_group_by.MasterGroupByActorAndCountConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	}, maxMessages, expectedEof, node_name)

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start client in a goroutine
	done := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		common_statefull_worker.RunWorker(master_group_by, master_group_by.Worker, "starting master group by actor count")
		done <- true
	}()

	// Wait for either completion or signal
	select {
	case sig := <-sigChan:
		if sig == syscall.SIGTERM {
			master_group_by.CloseWorker()
			log.Info("Worker shut down successfully")
			<-done
		} else {
			log.Warning("Signal %v not handled", sig)
		}
	case <-done:
		log.Info("Worker finished successfully")
	}

	wg.Wait()
}
