package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"distribuidos-tp1/filters/common_filter"
	filter "distribuidos-tp1/filters/filter_only_one_country"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_only_one_country")

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

	filter := filter.NewFilterByOnlyOneCountry(filter.FilterByOnlyOneCountryConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
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
		common_filter.RunWorker(filter, filter.Worker, "Starting filter by only one country")
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
