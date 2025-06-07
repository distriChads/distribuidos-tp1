package router

import (
	worker "distribuidos-tp1/common/worker/worker"
	"strconv"
	"strings"

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

func (r *Router) router() error {
	for {
		err, msg, input := worker.ReceivedMessages(r.Worker)
		if err != nil {
			log.Errorf("Error receiving messages: %s", err)
			return err
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

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|...
// ---------------------------------
const ID = 0
const TITLE = 1
const DATE = 2

func (f *Router) Filter(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		raw_year := strings.Split(parts[DATE], "-")[0]
		year, err := strconv.Atoi(raw_year)
		if err != nil {
			continue
		}
		if year >= 2000 {
			result = append(result, strings.TrimSpace(parts[ID])+worker.MESSAGE_SEPARATOR+strings.TrimSpace(parts[TITLE]))
		}
	}
	return result
}

func (f *Router) HandleEOF(client_id string) error {
	// if _, ok := f.eofs[client_id]; !ok {
	// 	f.eofs[client_id] = 0
	// }
	// f.eofs[client_id]++
	// if f.eofs[client_id] >= f.expected_eof {
	// 	log.Infof("Sending EOF for client %s", client_id)
	// 	for _, queue_name := range f.Worker.OutputExchange.RoutingKeys {
	// 		routing_key := queue_name
	// 		message := client_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
	// 		err := worker.SendMessage(f.Worker, message, routing_key)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	log.Infof("Client %s finished", client_id)
	// }
	return nil
}

func (f *Router) SendMessage(message_to_send []string, client_id string) error {
	for _, line := range message_to_send {
		// TODO: clean up commented code, the hasher node is used to send messages
		// parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		if len(message_to_send) != 0 {
			// id, err := strconv.Atoi(parts[ID])
			// if err != nil {
			// 	return err
			// }
			send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
			message := client_id + worker.MESSAGE_SEPARATOR + line
			err := worker.SendMessage(f.Worker, message, send_queue_key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *Router) RunWorker(starting_message string) error {
	err := f.init(starting_message)
	if err != nil {
		return err
	}
	return f.router()
}
