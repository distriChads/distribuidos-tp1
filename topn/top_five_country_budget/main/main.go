package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"os"
	"os/signal"
	"sync"
	"syscall"

	topn "distribuidos-tp1/topn/top_five_country_budget"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("top_five_country_budget")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	log_level := v.GetString("log.level")
	inputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.input.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.input.routingKeys")},
		QueueName:   "top_five_country_budget_queue",
	}
	outputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.output.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.output.routingKeys")},
		QueueName:   "top_five_country_budget_queue",
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

	topn := topn.NewTopFiveCountryBudget(topn.TopFiveCountryBudgetConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  inputExchangeSpec,
			OutputExchange: outputExchangeSpec,
			MessageBroker:  messageBroker,
		},
	})

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start client in a goroutine
	done := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		topn.RunWorker()
		done <- true
	}()

	// Wait for either completion or signal
	select {
	case sig := <-sigChan:
		if sig == syscall.SIGTERM {
			topn.CloseWorker()
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
