package rabbitmq

import (
	"logging"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Receiver struct {
	*RabbitClient
	Callbacks map[string]func(string)
}

func NewReceiver(host string, callbacks map[string]func(string)) (*Receiver, error) {
	var queues []string
	for queue := range callbacks {
		queues = append(queues, queue)
	}
	client, err := NewRabbitClient(host, queues)
	if err != nil {
		return nil, err
	}
	return &Receiver{RabbitClient: client, Callbacks: callbacks}, nil
}

func (r *Receiver) Run() error {
	for queue, callback := range r.Callbacks {
		msgs, err := r.Channel.Consume(
			queue, // queue
			"",    // consumer
			true,  // auto-ack
			false, // exclusive
			false, // no-local
			false, // no-wait
			nil,   // args
		)
		if err != nil {
			logging.Errorf("Failed to register a consumer: %s", err)
			return err
		}
		go func(queue string, ch <-chan amqp.Delivery) {
			for msg := range ch {
				logging.Infof("Received a message from %s: %s", queue, msg.Body)
				callback(string(msg.Body))
			}
		}(queue, msgs)
	}
	select {}
}
