package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	master_group_by "distribuidos-tp1/master_group_by/master_group_by_country_sum"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("master_group_by_country_sum")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	log_level := v.GetString("log.level")
	inputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.input.name"),
		RoutingKeys: strings.Split(v.GetString("worker.exchange.input.routingKeys"), ","),
		QueueName:   "master_group_by_country_sum_queue",
	}
	outputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.output.name"),
		RoutingKeys: strings.Split(v.GetString("worker.exchange.output.routingKeys"), ","),
		QueueName:   "master_group_by_country_sum_queue",
	}
	messageBroker := v.GetString("worker.broker")

	if inputExchangeSpec.Name == "" || inputExchangeSpec.RoutingKeys[0] == "" || outputExchangeSpec.Name == "" || outputExchangeSpec.RoutingKeys[0] == "" || messageBroker == "" {
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
	masterGroupByCountrySum := master_group_by.NewGroupByCountryAndSum(master_group_by.MasterGroupByCountryAndSumConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  inputExchangeSpec,
			OutputExchange: outputExchangeSpec,
			MessageBroker:  messageBroker,
		},
	}, maxMessages, expectedEof, storage_base_dir)

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start client in a goroutine
	done := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		masterGroupByCountrySum.RunWorker("starting master group by country sum")
		done <- true
	}()

	// Wait for either completion or signal
	select {
	case sig := <-sigChan:
		if sig == syscall.SIGTERM {
			masterGroupByCountrySum.CloseWorker()
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
