package router

import (
	worker "distribuidos-tp1/common/worker/worker"

	"github.com/op/go-logging"
)

type Router struct {
	worker.Worker
	RoutingMap
}

type RouterConfig struct {
	worker.WorkerConfig
}

var log = logging.MustGetLogger("router")

func (r *Router) init(starting_message string) error {
	log.Info(starting_message)
	err := worker.InitSender(&r.Worker)
	if err != nil {
		return err
	}

	err = worker.InitReceiver(&r.Worker)
	if err != nil {
		return err
	}

	return nil
}

func (r *Router) messageDispatcher() error {
	for {
		err, msg, inputIndex := worker.ReceivedMessages(r.Worker)
		if err != nil {
			log.Errorf("Error receiving messages: %s", err)
			return err
		}

		output_routing_keys, ok := r.RoutingMap[inputIndex]
		if !ok {
			log.Errorf("No routing keys found for input index %d", inputIndex)
			continue
		}
		for _, routing_key := range output_routing_keys {
			log.Debugf("Received message: %s, routing key: %s", msg, routing_key)
			err = worker.SendMessage(r.Worker, msg, routing_key)
			if err != nil {
				log.Errorf("Error sending message: %s", err)
				return err
			}
		}
	}
}

func NewRouter(config RouterConfig, routingMap RoutingMap) *Router {
	log.Infof("Router: %+v", config)
	return &Router{
		Worker: worker.Worker{
			Exchange:      config.Exchange,
			MessageBroker: config.MessageBroker,
		},
		RoutingMap: routingMap,
	}
}

func (r *Router) HandleEOF(client_id string) error {
	// TODO: implement EOF handling logic
	return nil
}

func (r *Router) SendMessage(message_to_send []string, client_id string) error {
	for _, line := range message_to_send {
		// TODO: clean up commented code, the hasher node is used to send messages
		// parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		if len(message_to_send) != 0 {
			// id, err := strconv.Atoi(parts[ID])
			// if err != nil {
			// 	return err
			// }
			send_queue_key := r.Worker.Exchange.OutputRoutingKeys[0]
			message := client_id + worker.MESSAGE_SEPARATOR + line
			err := worker.SendMessage(r.Worker, message, send_queue_key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Router) RunWorker(starting_message string) error {
	err := r.init(starting_message)
	if err != nil {
		return err
	}
	return r.messageDispatcher()
}
