package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Sender struct {
	*RabbitClient
}

func NewSender(host string, queues []string) (*Sender, error) {
	client, err := NewRabbitClient(host, queues)
	if err != nil {
		return nil, err
	}
	return &Sender{RabbitClient: client}, nil
}

func (s *Sender) SendTo(queue string, body []byte) error {
	if s.Channel == nil {
		return fmt.Errorf("channel is nil")
	}
	return s.Channel.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body, // TODO: change to []byte if is needed
		})
}
