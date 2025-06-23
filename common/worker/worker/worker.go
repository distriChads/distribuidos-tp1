package worker

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	MESSAGE_SEPARATOR       = "|"
	MESSAGE_ARRAY_SEPARATOR = ","
	MESSAGE_EOF             = "EOF"

	EXCHANGE_NAME = "data_exchange"
	EXCHANGE_TYPE = "topic"

	INPUTS_COMMON_NODES = 1
	INPUTS_JOINER_NODES = 2
	INPUTS_ROUTER_NODES = 7
)

var log = logging.MustGetLogger("worker")

type sender struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

type receiver struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	queues   []amqp.Queue
	messages []<-chan amqp.Delivery
}

type ExchangeSpec struct {
	InputRoutingKeys  []string
	OutputRoutingKeys map[string][]string
	QueueName         string
}

type WorkerConfig struct {
	Exchange      ExchangeSpec
	MessageBroker string
}

type Worker struct {
	Exchange      ExchangeSpec
	MessageBroker string
	sender        *sender
	receiver      *receiver
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

func NewWorker(config WorkerConfig, prefetch_count int) (*Worker, error) {
	log.Infof("Worker: %+v", config)
	worker := Worker{
		Exchange:      config.Exchange,
		MessageBroker: config.MessageBroker,
		sender:        nil,
		receiver:      nil,
	}

	err := worker.initSender()
	if err != nil {
		log.Errorf("Error initializing sender: %s", err)
		return nil, err
	}

	err = worker.initReceiver(prefetch_count)
	if err != nil {
		log.Errorf("Error initializing sender: %s", err)
		return nil, err
	}

	return &worker, nil
}

func (w *Worker) initSender() error {
	conn, err := initConnection(w.MessageBroker)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	err = ch.ExchangeDeclare(
		EXCHANGE_NAME, // name
		EXCHANGE_TYPE, // type
		false,         // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return err
	}

	w.sender = &sender{
		conn: conn,
		ch:   ch,
	}

	log.Info("Sender initialized")
	return nil
}

func (w *Worker) initReceiver(prefetch_count int) error {
	conn, err := initConnection(w.MessageBroker)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	err = ch.Qos(
		prefetch_count, // prefetch count
		0,              // prefetch size
		false,          // global
	)
	if err != nil {
		return err
	}

	err = ch.ExchangeDeclare(
		EXCHANGE_NAME, // name
		EXCHANGE_TYPE, // type
		false,         // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return err
	}

	var queues []amqp.Queue
	var messages []<-chan amqp.Delivery

	for _, routingKey := range w.Exchange.InputRoutingKeys {
		q, err := ch.QueueDeclare(
			routingKey, // name
			false,      // durable
			false,      // delete when unused
			false,      // exclusive
			false,      // no-wait
			nil,        // arguments
		)
		if err != nil {
			return err
		}

		err = ch.QueueBind(
			q.Name,        // queue name
			routingKey,    // routing key
			EXCHANGE_NAME, // exchange
			false,
			nil,
		)
		log.Debugf("QueueBind: queue name: %s, routing key: %s, exchange name: %s, err: %v", q.Name, routingKey, EXCHANGE_NAME, err)
		if err != nil {
			return err
		}

		msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			return err
		}

		queues = append(queues, q)
		messages = append(messages, msgs)
	}

	w.receiver = &receiver{
		conn:     conn,
		ch:       ch,
		queues:   queues,
		messages: messages,
	}

	log.Info("Receiver initialized")
	return nil
}

func (w *Worker) SendMessage(message string, routingKey string) error {
	if w.sender == nil {
		return errors.New("sender not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := w.sender.ch.PublishWithContext(ctx,
		EXCHANGE_NAME, // exchange
		routingKey,    // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		return err
	}
	log.Debugf("Sent message to (routing key: %s): %s", routingKey, message)

	return nil
}

func (w *Worker) ReceivedMessages(ctx context.Context) (amqp.Delivery, int, error) {
	if w.receiver == nil {
		return amqp.Delivery{}, 0, errors.New("receiver not initialized")
	}

	msgChans := w.receiver.messages
	numInputs := len(msgChans)

	selectCases := make([]reflect.SelectCase, numInputs+1)

	// First case: cancell context
	selectCases[0] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}

	// Add cases for each message channel
	for i, ch := range msgChans {
		selectCases[i+1] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}

	chosen, recv, ok := reflect.Select(selectCases)
	if chosen == 0 {
		return amqp.Delivery{}, 0, ctx.Err()
	}
	if !ok {
		return amqp.Delivery{}, 0, errors.New("channel closed unexpectedly")
	}

	msg := recv.Interface().(amqp.Delivery)
	return msg, chosen - 1, nil // -1 porque el 0 era el ctx.Done()
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
