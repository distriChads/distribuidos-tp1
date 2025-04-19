package rabbitmq

import (
	"logging"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Queues  []string
}

func NewRabbitClient(host string, queues []string) (*RabbitClient, error) {
	conn, err := amqp.Dial(host)
	if err != nil {
		logging.Errorf("Failed to connect to RabbitMQ: %s", err)
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		logging.Errorf("Failed to open a channel: %s", err)
		return nil, err
	}
	for _, queue := range queues {
		_, err := ch.QueueDeclare(
			queue, // name
			false, // durable TODO: change to true when used to persist messages
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			logging.Errorf("Failed to declare a queue: %s", err)
			return nil, err
		}
	}

	return &RabbitClient{Conn: conn, Channel: ch, Queues: queues}, nil
}

func (r *RabbitClient) Close() {
	if err := r.Channel.Close(); err != nil {
		logging.Errorf("Failed to close channel: %s", err)
	}
	if err := r.Conn.Close(); err != nil {
		logging.Errorf("Failed to close connection: %s", err)
	}
}
