package router

import (
	"context"
	worker "distribuidos-tp1/common/worker/worker"
	"errors"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

const (
	FIELD_SEPARATOR = "|"
	VALUE_SEPARATOR = ","
	LINE_SEPARATOR  = "\n"
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

		msgBody := string(msg.Body)

		output_routing_keys, ok := r.RoutingMap[inputIndex]
		if !ok {
			log.Errorf("No routing keys found for input index %d", inputIndex)
			msg.Ack(false)
			continue
		}

		dataDistributed := r.routeDataByMovieId(output_routing_keys, msgBody)

		for i, routing_key := range output_routing_keys {
			dataToSend := dataDistributed[i]
			if len(dataToSend) == 0 {
				continue
			}
			log.Infof("Sending message: %s, from input %d, to routing key: %s", dataToSend, inputIndex, routing_key)

			err = r.worker.SendMessage(dataToSend, routing_key)
			if err != nil {
				log.Errorf("Error sending message: %s", err)
				return err
			}
		}
		msg.Ack(false)
	}
}

func (r *Router) routeDataByMovieId(outputRoutingKeys []string, inputData string) []string {
	dataDistributed := make([]string, len(outputRoutingKeys))

	splitedData := strings.SplitN(inputData, FIELD_SEPARATOR, 2)
	clientId, message := splitedData[0], splitedData[1]
	lines := strings.Split(message, LINE_SEPARATOR)

	if message == worker.MESSAGE_EOF {
		log.Infof("Received EOF message from client %s", clientId)
		for i := range dataDistributed {
			dataDistributed[i] = clientId + FIELD_SEPARATOR + worker.MESSAGE_EOF
		}
		return dataDistributed
	}

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		movieIdString := strings.SplitN(line, FIELD_SEPARATOR, 2)[0]
		if len(movieIdString) == 0 {
			log.Warningf("Empty movie ID in line: %s", line)
			continue
		}
		movieId, err := strconv.Atoi(movieIdString)
		if err != nil {
			log.Errorf("Invalid movie ID '%s' in line: %s", movieIdString, line)
			continue
		}

		indexForSharding := movieId % len(outputRoutingKeys)
		if len(dataDistributed[indexForSharding]) == 0 {
			dataDistributed[indexForSharding] = clientId + FIELD_SEPARATOR + line
		} else {
			dataDistributed[indexForSharding] += LINE_SEPARATOR + line
		}
	}

	return dataDistributed
}
