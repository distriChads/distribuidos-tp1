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

	router "distribuidos-tp1/router"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("router")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	log_level := v.GetString("log.level")
	messageBroker := v.GetString("worker.broker")

	routingMap := router.GetRoutingMap(v)

	exchangeSpec := worker.ExchangeSpec{
		InputRoutingKeys:  strings.Split(v.GetString("worker.exchange.input.routingkeys"), ","),
		OutputRoutingKeys: strings.Split("hola", "adios"),
		QueueName:         "router",
	}

	if exchangeSpec.InputRoutingKeys[0] == "" || exchangeSpec.OutputRoutingKeys[0] == "" || messageBroker == "" {
		log.Criticalf("Error: one or more environment variables are empty")
		return
	}

	if err := utils.InitLogger(log_level); err != nil {
		log.Criticalf("%s", err)
		return
	}

	router := router.NewRouter(router.RouterConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	},
		routingMap,
	)
	if router == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Set up signal handling
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
		router.RunWorker(ctx, "Starting router")
		done <- true
	}()

	// Wait for either completion or signal
	select {
	case sig := <-sigChan:
		log.Infof("Received signal: %s, shutting down...", sig)
		cancel() // Gracefully shut down
		<-done
	case <-done:
		log.Info("Worker finished successfully")
	}

	wg.Wait()
	log.Info("Router has shut down gracefully")
}
