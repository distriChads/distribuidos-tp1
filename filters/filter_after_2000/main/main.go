package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

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

	log_level := v.GetString("log.level")
	queueName := v.GetString("worker.queue.name")
	inputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.input.name"),
		RoutingKeys: []string{v.GetString("worker.exchange.input.routingkeys")},
		QueueName:   queueName,
	}
	outputExchangeSpec := worker.ExchangeSpec{
		Name:        v.GetString("worker.exchange.output.name"),
		RoutingKeys: strings.Split(v.GetString("worker.exchange.output.routingkeys"), ","),
		QueueName:   queueName,
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
	expectedEof := v.GetInt("worker.expectedeof")
	if expectedEof == 0 {
		expectedEof = 1
	}

	filter := filter.NewFilterByAfterYear2000(filter.FilterByAfterYear2000Config{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  inputExchangeSpec,
			OutputExchange: outputExchangeSpec,
			MessageBroker:  messageBroker,
		},
	}, expectedEof)

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start client in a goroutine
	done := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		filter.RunWorker()
		done <- true
	}()

	// Wait for either completion or signal
	select {
	case sig := <-sigChan:
		if sig == syscall.SIGTERM {
			filter.CloseWorker()
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
