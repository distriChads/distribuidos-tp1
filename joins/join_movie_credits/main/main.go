package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	join "distribuidos-tp1/joins/join_movie_credits"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("join_movie_credits")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		return
	}

	log_level := v.GetString("log.level")

	queueName := v.GetString("worker.queue.name")
	exchange := worker.ExchangeSpec{
		InputRoutingKeys:  strings.Split(v.GetString("worker.exchange.input.routingkeys"), ","),
		OutputRoutingKeys: strings.Split(v.GetString("worker.exchange.output.routingkeys"), ","),
		QueueName:         queueName,
	}
	messageBroker := v.GetString("worker.broker")

	if exchange.InputRoutingKeys[0] == "" || exchange.OutputRoutingKeys[0] == "" || messageBroker == "" {
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
	expectedEof := v.GetInt("worker.expectedeof")
	if expectedEof == 0 {
		expectedEof = 1
	}

	join := join.NewJoinMovieCreditsById(join.JoinMovieCreditsByIdConfig{
		WorkerConfig: worker.WorkerConfig{
			Exchange:      exchange,
			MessageBroker: messageBroker,
		},
	}, maxMessages, expectedEof)

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start client in a goroutine
	done := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		join.RunWorker()
		done <- true
	}()

	// Wait for either completion or signal
	select {
	case sig := <-sigChan:
		if sig == syscall.SIGTERM {
			join.CloseWorker()
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
