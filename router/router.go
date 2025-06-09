package router

import (
	"context"
	worker "distribuidos-tp1/common/worker/worker"
	"errors"

	"github.com/op/go-logging"
)

type Router struct {
	worker worker.Worker
	RoutingMap
}

type RouterConfig struct {
	worker.WorkerConfig
}

var log = logging.MustGetLogger("router")

func NewRouter(config RouterConfig, routingMap RoutingMap) *Router {
	log.Infof("Router: %+v", config)
	worker, err := worker.NewWorker(config.WorkerConfig)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &Router{
		worker:     *worker,
		RoutingMap: routingMap,
	}
}

func (r *Router) HandleEOF(client_id string) error {
	// TODO: implement EOF handling logic (control exchange)
	return nil
}

func (r *Router) RunWorker(ctx context.Context, starting_message string) error {
	log.Info(starting_message)

	for {
		msg, inputIndex, err := r.worker.ReceivedMessages(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("Shutting down message dispatcher gracefully")
				return nil
			}
			log.Errorf("Error receiving messages: %s", err)
			return err
		}

		output_routing_keys, ok := r.RoutingMap[inputIndex]
		if !ok {
			log.Errorf("No routing keys found for input index %d", inputIndex)
			msg.Ack(false)
			continue
		}

		for _, routing_key := range output_routing_keys {
			log.Debugf("Sending message: %s, routing key: %s", string(msg.Body), routing_key)
			err = r.worker.SendMessage(string(msg.Body), routing_key)
			if err != nil {
				log.Errorf("Error sending message: %s", err)
				return err
			}
		}
		msg.Ack(false)
	}
}
