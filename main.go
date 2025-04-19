package main

import (
	"distribuidos-tp1/common/worker/worker"
	"os"

	"distribuidos-tp1/filters/filter_argentina"
	"distribuidos-tp1/filters/filter_spain_2000"
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
		"filter_argentina_output", // name
		"fanout",                  // type
		true,                      // durable
		false,                     // auto-deleted
		false,                     // internal
		false,                     // no-wait
		nil,                       // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"filter_spain_output", // name
		"fanout",              // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"movies", // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	filterArgentina := filter_argentina.NewFilterByArgentina(filter_argentina.FilterByArgentinaConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "movies",
			OutputExchange: "filter_argentina_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	filterSpain := filter_spain_2000.NewFilterBySpainAndOf2000(filter_spain_2000.FilterBySpainAndOf2000Config{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "filter_argentina_output",
			OutputExchange: "filter_spain_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	defer filterArgentina.CloseWorker()
	defer filterSpain.CloseWorker()

	outputQueue, _ := ch.QueueDeclare(
		"output_consumer", // name
		false,             // durable
		true,              // delete when unused
		true,              // exclusive
		false,             // no-wait
		nil,               // arguments
	)

	_ = ch.QueueBind(
		outputQueue.Name,      // queue name
		"",                    // routing key
		"filter_spain_output", // exchange
		false,                 // no-wait
		nil,                   // arguments
	)

	msgs, _ := ch.Consume(
		outputQueue.Name, // queue
		"",               // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)

	go filterArgentina.RunWorker()
	time.Sleep(2 * time.Second)

	go filterSpain.RunWorker()
	time.Sleep(2 * time.Second)

	for msg := range msgs {
		println("Received:", string(msg.Body))
		f, _ := os.OpenFile("test_query.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		f.WriteString(string(msg.Body) + "\n")
	}

	ch.ExchangeDelete("filter_only_one_output_exchange", false, false)
	println("Exchanges deleted")
}
