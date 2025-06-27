package common_filter

import (
	"distribuidos-tp1/common/utils"
	buffer "distribuidos-tp1/common/worker/hasher"
	"distribuidos-tp1/common/worker/worker"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("common_filter")

// ========================== Testing ==========================
const (
	TEST_SEND_MULTIPLE_EOF     = 1
	TEST_SEND_MULTIPLE_MESSAGE = 2
)

var sent_multiple_message = false

// ========================== Testing ==========================

type CommonFilter struct {
	Worker *worker.Worker
	Buffer *buffer.HasherContainer
}

func NewCommonFilter(config worker.WorkerConfig) *CommonFilter {
	log.Infof("FilterByAfterYear2000: %+v", config)
	worker, err := worker.NewWorker(config, 1)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	dict := make(map[string]int)
	for nodeType, routingKeys := range config.Exchange.OutputRoutingKeys {
		dict[nodeType] = len(routingKeys)
	}
	buffer := buffer.NewHasherContainer(dict)

	return &CommonFilter{
		Worker: worker,
		Buffer: buffer,
	}
}

// Send the filtered message to the next nodes
// the send works as follow:
// first we pass the node_type this is for example the FilterArgentina that needs to send its message
// to a node_type = FilterMovies2000Spain and to node_type = FilterMovieAfter2000
// then we get the message to send with the hasher-buffer (it's not a state, its a buffer from only the message that is now being filtered)
// and this buffer gives us the message and the key index to send the message to
func (f *CommonFilter) SendMessage(client_id string, message_id string) error {
	for node_type := range f.Worker.Exchange.OutputRoutingKeys {
		messages_to_send := f.Buffer.GetMessages(node_type)
		for routing_key_index, message := range messages_to_send {
			if len(message) != 0 {
				routing_key := f.Worker.Exchange.OutputRoutingKeys[node_type][routing_key_index]
				message = client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + message
				err := f.Worker.SendMessage(message, routing_key)
				if err != nil {
					return err
				}

				// ========================== Testing ==========================
				if utils.TestCase == TEST_SEND_MULTIPLE_MESSAGE {
					if sent_multiple_message {
						continue
					}
					err = f.Worker.SendMessage(message, routing_key)
					if err != nil {
						return err
					}
					sent_multiple_message = true
				}
				// ========================== Testing ==========================

				log.Debugf("Sent message to output exchange: %s", message)
			}
		}

	}
	return nil
}

// Send the eof to all next nodes
func (f *CommonFilter) HandleEOF(client_id string, message_id string) error {
	for _, output_routing_keys := range f.Worker.Exchange.OutputRoutingKeys {
		for _, output_key := range output_routing_keys {
			message := client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
			err := f.Worker.SendMessage(message, output_key)
			if err != nil {
				return err
			}
			// ========================== Testing ==========================
			if utils.TestCase == TEST_SEND_MULTIPLE_EOF {
				err = f.Worker.SendMessage(message, output_key)
				if err != nil {
					return err
				}
				log.Infof("Sent repeated EOF to output exchange: %s", message)
			}
			// ========================== Testing ==========================
		}
	}
	return nil
}

// close the worker
func (f *CommonFilter) CloseWorker() {
	if f.Worker != nil {
		f.Worker.CloseWorker()
	}
	log.Info("Filter worker closed")
}
