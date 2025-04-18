package main

import (
	"context"
	"distribuidos-tp1/common/worker"
	"distribuidos-tp1/filters/filter_after_2000"
	"distribuidos-tp1/filters/filter_argentina"
	"fmt"
	"time"

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
		"argentina_input_exchange", // name
		"fanout",                   // type
		true,                       // durable
		false,                      // auto-deleted
		false,                      // internal
		false,                      // no-wait
		nil,                        // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"argentina_output_exchange", // name
		"fanout",                    // type
		true,                        // durable
		false,                       // auto-deleted
		false,                       // internal
		false,                       // no-wait
		nil,                         // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"after_2000_output_exchange", // name
		"fanout",                     // type
		true,                         // durable
		false,                        // auto-deleted
		false,                        // internal
		false,                        // no-wait
		nil,                          // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	filterArgentina := filter_argentina.NewFilterByArgentina(filter_argentina.FilterByArgentinaConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "argentina_input_exchange",
			OutputExchange: "argentina_output_exchange",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	filterAfterYear2000 := filter_after_2000.NewFilterByAfterYear2000(filter_after_2000.FilterByAfterYear2000Config{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "argentina_output_exchange",
			OutputExchange: "after_2000_output_exchange",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	defer filterAfterYear2000.CloseWorker()
	defer filterArgentina.CloseWorker()

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
		outputQueue.Name,             // queue name
		"",                           // routing key
		"after_2000_output_exchange", // exchange
		false,                        // no-wait
		nil,                          // arguments
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

	go filterArgentina.RunWorker()
	time.Sleep(2 * time.Second)
	go filterAfterYear2000.RunWorker()
	time.Sleep(2 * time.Second)

	// Produce 3 messages to input_exchange
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send 3 sample messages to the input exchange

	line := `
	USA|ARG|CHI,2006-10-30,Comedy|Family|Action,toy story,id1
	CAN|ARG|BRA,1996-10-30,Comedy|Family|Action,megamente,id2
	ARG|CHI|SPAIN,2004-10-30,Comedy|Family|Action,shrek,id3
	USA,2008-10-30,Comedy|Family|Action,cars,id4
	`

	for i := 1; i <= 3; i++ {
		message := fmt.Sprintf("%v", line)
		err = ch.PublishWithContext(ctx,
			"argentina_input_exchange", // exchange
			"",                         // routing key
			false,                      // mandatory
			false,                      // immediate
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

	ch.ExchangeDelete("argentina_input_exchange", false, false)
	ch.ExchangeDelete("argentina_output_exchange", false, false)
	ch.ExchangeDelete("after_2000_output_exchange", false, false)
	println("Exchanges deleted")
}
