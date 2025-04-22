package worker

import (
	"context"
	"errors"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const MESSAGE_SEPARATOR = "|"
const MESSAGE_ARRAY_SEPARATOR = ","
const MESSAGE_EOF = "EOF"

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

type ExchangeSpec struct {
	Name        string
	RoutingKeys []string
	QueueName   string
}

type WorkerConfig struct {
	InputExchange       ExchangeSpec
	SecondInputExchange ExchangeSpec
	OutputExchange      ExchangeSpec
	MessageBroker       string
}

type Worker struct {
	InputExchange       ExchangeSpec
	SecondInputExchange ExchangeSpec
	OutputExchange      ExchangeSpec
	MessageBroker       string
	sender              *sender
	receiver            *receiver
	secondReceiver      *receiver // solo necesario para los joins que van a tener 2 receivers :)
}

func initConnection(broker string) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error

	max_retries := 3 // TODO: make these env variables
	retry_sleep := 10 * time.Second
	backoff_factor := 2
	for i := range max_retries {
		conn, err = amqp.Dial(broker)
		if err == nil {
			break
		}
		log.Warningf("Failed to connect to broker on attempt %d: %s", i+1, err)
		if i < 2 {
			time.Sleep(time.Duration(i*backoff_factor) + retry_sleep)
		}
	}
	if err != nil {
		log.Errorf("Failed to connect to broker: %s", err)
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

	err = ch.ExchangeDeclare(
		worker.OutputExchange.Name, // name
		"topic",                    // type
		false,                      // durable
		true,                       // auto-deleted
		false,                      // internal
		false,                      // no-wait
		nil,                        // arguments
	)
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

	err = ch.ExchangeDeclare(
		"topic",                   // type
		false,                     // durable
		true,                      // auto-deleted
		false,                     // internal
		false,                     // no-wait
		nil,                       // arguments
	)
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		worker.InputExchange.QueueName, // name
		false,                          // durable
		false,                          // delete when unused
		true,                           // exclusive
		false,                          // no-wait
		nil,                            // arguments
	)
	if err != nil {
		return err
	}

	for _, routingKey := range worker.InputExchange.RoutingKeys {
		err = ch.QueueBind(
			q.Name,                    // queue name
			routingKey,                // routing key
			worker.InputExchange.Name, // exchange
			false,
			nil,
		)
		if err != nil {
			return err
		}
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

func InitSecondReceiver(worker *Worker) error {
	conn, err := initConnection(worker.MessageBroker)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	err = ch.ExchangeDeclare(
		"topic",                         // type
		false,                           // durable
		true,                            // auto-deleted
		false,                           // internal
		false,                           // no-wait
		nil,                             // arguments
	)
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		worker.SecondInputExchange.QueueName, // name
		false,                                // durable
		false,                                // delete when unused
		true,                                 // exclusive
		false,                                // no-wait
		nil,                                  // arguments
	)
	if err != nil {
		return err
	}

	for _, routingKey := range worker.SecondInputExchange.RoutingKeys {
		err = ch.QueueBind(
			q.Name,                          // queue name
			routingKey,                      // routing key
			worker.SecondInputExchange.Name, // exchange
			false,
			nil,
		)
		if err != nil {
			return err
		}
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

	worker.secondReceiver = &receiver{
		conn:     conn,
		ch:       ch,
		queue:    q,
		messages: msgs,
	}

	log.Info("Second Receiver initialized")
	return nil
}

func SendMessage(worker Worker, message string) error {
	if worker.sender == nil {
		return errors.New("sender not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, routingKey := range worker.OutputExchange.RoutingKeys {
		err := worker.sender.ch.PublishWithContext(ctx,
			worker.OutputExchange.Name, // exchange
			routingKey,                 // routing key
			false,                      // mandatory
			false,                      // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			})
		if err != nil {
			return err
		}
		log.Debugf("Sent message to exchange %s (routing key: %s): %s", worker.OutputExchange.Name, routingKey, message)
	}

	return nil
}

func ReceivedMessages(worker Worker) (<-chan amqp.Delivery, error) {
	if worker.receiver == nil {
		return nil, errors.New("receiver not initialized")
	}

	return worker.receiver.messages, nil
}

func SecondReceivedMessages(worker Worker) (<-chan amqp.Delivery, error) {
	if worker.secondReceiver == nil {
		return nil, errors.New("receiver not initialized")
	}

	return worker.secondReceiver.messages, nil
}

func RunWorker(worker Worker) error {
	return errors.New("not implemented")
}

func CloseWorker(worker *Worker) error {
	return errors.New("not implemented")
}

func (worker *Worker) CloseWorker() error {
	err := CloseSender(worker)
	if err != nil {
		return err
	}

	return CloseReceiver(worker)
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

func CloseSecondReceiver(worker *Worker) error {
	if worker.secondReceiver == nil {
		return errors.New("receiver not initialized")
	}

	err := worker.secondReceiver.ch.Close()
	if err != nil {
		return err
	}

	err = worker.secondReceiver.conn.Close()
	if err != nil {
		return err
	}

	worker.secondReceiver = nil
	return nil
}
