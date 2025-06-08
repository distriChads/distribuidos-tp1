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

	group_by "distribuidos-tp1/group_by/group_by_overview_average"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("group_by_overview_average")

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
	if maxMessages == 0 {
		maxMessages = 10
	}

	node_name := v.GetString("worker.nodename")
	group_by := group_by.NewGroupByOverviewAndAvg(group_by.GroupByOverviewAndAvgConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchangeSpec,
			MessageBroker: messageBroker,
		},
	}, maxMessages, node_name)

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start client in a goroutine
	done := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		common_statefull_worker.RunWorker(group_by, group_by.Worker, "Starting group by overview average")
		done <- true
	}()

	// Wait for either completion or signal
	select {
	case sig := <-sigChan:
		if sig == syscall.SIGTERM {
			group_by.CloseWorker()
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
