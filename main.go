package main

import (
	"context"
	"distribuidos-tp1/common/worker"
	"distribuidos-tp1/filters/filter_only_one_country"
	"distribuidos-tp1/group_by/group_by_country_sum"
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
		"result_group_by_output_exchange", // name
		"fanout",                          // type
		true,                              // durable
		false,                             // auto-deleted
		false,                             // internal
		false,                             // no-wait
		nil,                               // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"filter_only_one_output_exchange", // name
		"fanout",                          // type
		true,                              // durable
		false,                             // auto-deleted
		false,                             // internal
		false,                             // no-wait
		nil,                               // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"filter_only_one_input_exchange", // name
		"fanout",                         // type
		true,                             // durable
		false,                            // auto-deleted
		false,                            // internal
		false,                            // no-wait
		nil,                              // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	/*filterArgentina := filter_argentina.NewFilterByArgentina(filter_argentina.FilterByArgentinaConfig{
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
	})*/

	filterOnlyOneCountry := filter_only_one_country.NewFilterByOnlyOneCountry(filter_only_one_country.FilterByOnlyOneCountryConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "filter_only_one_input_exchange",
			OutputExchange: "filter_only_one_output_exchange",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	groupByCountryAndSum := group_by_country_sum.NewGroupByCountryAndSum(group_by_country_sum.GroupByCountryAndSumConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "filter_only_one_output_exchange",
			OutputExchange: "result_group_by_output_exchange",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	defer filterOnlyOneCountry.CloseWorker()
	defer groupByCountryAndSum.CloseWorker()

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
		outputQueue.Name,                  // queue name
		"",                                // routing key
		"result_group_by_output_exchange", // exchange
		false,                             // no-wait
		nil,                               // arguments
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

	go filterOnlyOneCountry.RunWorker()
	time.Sleep(2 * time.Second)
	go groupByCountryAndSum.RunWorker()
	time.Sleep(2 * time.Second)

	// Produce 3 messages to input_exchange
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send 3 sample messages to the input exchange

	/*line = `
	USA|ARG|CHI,2006-10-30,Comedy|Family|Action,toy story,id1
	CAN|ARG|BRA,1996-10-30,Comedy|Family|Action,megamente,id2
	ARG|CHI|SPAIN,2004-10-30,Comedy|Family|Action,shrek,id3
	USA,2008-10-30,Comedy|Family|Action,cars,id4
	`*/
	line := `
	USA|ARG|CHI,1500
	CAN|ARG|BRA,2000
	ARG|CHI|SPAIN,3000
	USA,1000
	ARG,4000
	CHI,6000
	USA,4000
	ARG,500
	BRA|CHI,700
	BRA,800
	`

	for i := 1; i <= 3; i++ {
		message := fmt.Sprintf("%v", line)
		err = ch.PublishWithContext(ctx,
			"filter_only_one_input_exchange", // exchange
			"",                               // routing key
			false,                            // mandatory
			false,                            // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			})
		if err != nil {
			println("Failed to publish message:", err.Error())
			return
		}
		if i == 3 {
			message = "EOF"
			err = ch.PublishWithContext(ctx,
				"filter_only_one_input_exchange", // exchange
				"",                               // routing key
				false,                            // mandatory
				false,                            // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(message),
				})
			if err != nil {
				println("Failed to publish message:", err.Error())
				return
			}
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

	ch.ExchangeDelete("filter_only_one_input_exchange", false, false)
	ch.ExchangeDelete("filter_only_one_output_exchange", false, false)
	ch.ExchangeDelete("result_group_by_output_exchange", false, false)
	println("Exchanges deleted")
}
