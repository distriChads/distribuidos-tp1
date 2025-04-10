package main

import (
	"context"
	worker "distribuidos-tp1/common/worker"
	"fmt"
	"time"

	filters "distribuidos-tp1/fiters"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		println(err.Error())
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		println(err.Error())
		return
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"input_exchange", // name
		"fanout",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"output_exchange", // name
		"fanout",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	filter := filters.NewFilterByYear(filters.FilterByYearConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "input_exchange",
			OutputExchange: "output_exchange",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
		Year: 2020,
	})
	defer filter.CloseWorker()

	// Set up a consumer for the output exchange
	outputQueue, err := ch.QueueDeclare(
		"output_consumer", // name
		false,             // durable
		true,              // delete when unused
		true,              // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		println("Failed to declare output queue:", err.Error())
		return
	}

	err = ch.QueueBind(
		outputQueue.Name,  // queue name
		"",                // routing key
		"output_exchange", // exchange
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		println("Failed to bind output queue:", err.Error())
		return
	}

	msgs, err := ch.Consume(
		outputQueue.Name, // queue
		"",               // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		println("Failed to register consumer:", err.Error())
		return
	}

	go filter.RunWorker()
	time.Sleep(2 * time.Second)

	// Produce 3 messages to input_exchange
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send 3 sample messages to the input exchange
	for i := 1; i <= 3; i++ {
		message := fmt.Sprintf("Test message %d", i)
		err = ch.PublishWithContext(ctx,
			"input_exchange", // exchange
			"",               // routing key
			false,            // mandatory
			false,            // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			})
		if err != nil {
			println("Failed to publish message:", err.Error())
			return
		}
		println("Published message:", message)
		time.Sleep(500 * time.Millisecond) // Small delay between messages
	}

	// Wait a moment to allow processing
	println("Waiting for messages to be processed...")
	time.Sleep(2 * time.Second)

	// Read 3 messages from output_exchange
	println("Reading messages from output_exchange:")
	messageCount := 0
	for msg := range msgs {
		println("Received:", string(msg.Body))
		messageCount++
		if messageCount >= 3 {
			break
		}
	}

	ch.ExchangeDelete("input_exchange", false, false)
	ch.ExchangeDelete("output_exchange", false, false)
	println("Exchanges deleted")
}
