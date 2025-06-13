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

	topn "distribuidos-tp1/topn/first_and_last"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("first_and_last")

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
		QueueName:         "first_and_last",
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

	topn := topn.NewFirstAndLast(topn.FirstAndLastConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	}, maxMessages)
	if topn == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool)
	wg := sync.WaitGroup{}
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
