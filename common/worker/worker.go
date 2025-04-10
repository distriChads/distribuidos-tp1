package worker

import (
	"context"
	"errors"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("worker")

type sender struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

type receiver struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	queue    amqp.Queue
	messages <-chan amqp.Delivery
}

type WorkerConfig struct {
	InputExchange  string
	OutputExchange string
	MessageBroker  string
}

type Worker struct {
	InputExchange  string
	OutputExchange string
	MessageBroker  string
	sender         *sender
	receiver       *receiver
}

func initConnection(broker string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(broker)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func InitSender(worker *Worker) error {
	conn, err := initConnection(worker.MessageBroker)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	worker.sender = &sender{
		conn: conn,
		ch:   ch,
	}

	log.Info("Sender initialized")
	return nil
}

func InitReceiver(worker *Worker) error {
	conn, err := initConnection(worker.MessageBroker)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		worker.InputExchange, // name
		false,                // durable
		false,                // delete when unused
		true,                 // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		q.Name,               // queue name
		"",                   // routing key
		worker.InputExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	worker.receiver = &receiver{
		conn:     conn,
		ch:       ch,
		queue:    q,
		messages: msgs,
	}

	log.Info("Receiver initialized")
	return nil
}

func SendMessage(worker Worker, message []byte) error {
	if worker.sender == nil {
		return errors.New("sender not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := worker.sender.ch.PublishWithContext(ctx,
		worker.OutputExchange, // exchange
		"",                    // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})

	if err != nil {
		return err
	}

	log.Infof("Message sent: %s", string(message))
	return nil
}

func ReceivedMessages(worker Worker) (<-chan amqp.Delivery, error) {
	if worker.receiver == nil {
		return nil, errors.New("receiver not initialized")
	}

	return worker.receiver.messages, nil
}

func RunWorker(worker Worker) error {
	return errors.New("not implemented")
}

func CloseWorker(worker *Worker) error {
	return errors.New("not implemented")
}

func CloseSender(worker *Worker) error {
	if worker.sender == nil {
		return errors.New("sender not initialized")
	}

	err := worker.sender.ch.Close()
	if err != nil {
		return err
	}

	err = worker.sender.conn.Close()
	if err != nil {
		return err
	}

	worker.sender = nil
	return nil
}

func CloseReceiver(worker *Worker) error {
	if worker.receiver == nil {
		return errors.New("receiver not initialized")
	}

	err := worker.receiver.ch.Close()
	if err != nil {
		return err
	}

	err = worker.receiver.conn.Close()
	if err != nil {
		return err
	}

	worker.receiver = nil
	return nil
}
