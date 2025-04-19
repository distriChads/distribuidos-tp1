package main

import (
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/group_by_country_sum"
	"distribuidos-tp1/topn/top_five_country_budget"
	"os"

	"distribuidos-tp1/filters/filter_argentina"
	"distribuidos-tp1/filters/filter_only_one_country"
	"distribuidos-tp1/filters/filter_spain_2000"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func run_test_for_query_1() {
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
		f, _ := os.OpenFile("test_query1.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		f.WriteString(string(msg.Body) + "\n")
	}

	ch.ExchangeDelete("filter_only_one_output_exchange", false, false)
	println("Exchanges deleted")
}

func run_test_for_query_2() {
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
		"filter_only_one_country_output", // name
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

	err = ch.ExchangeDeclare(
		"group_by_country_output", // name
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
		"get_top_5_output", // name
		"fanout",           // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
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

	filterOneCountry := filter_only_one_country.NewFilterByOnlyOneCountry(filter_only_one_country.FilterByOnlyOneCountryConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "movies",
			OutputExchange: "filter_only_one_country_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	groupByCountry := group_by_country_sum.NewGroupByCountryAndSum(group_by_country_sum.GroupByCountryAndSumConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "filter_only_one_country_output",
			OutputExchange: "group_by_country_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	getTop5 := top_five_country_budget.NewTopFiveCountryBudget(top_five_country_budget.TopFiveCountryBudgetConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "group_by_country_output",
			OutputExchange: "get_top_5_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	defer filterOneCountry.CloseWorker()
	defer groupByCountry.CloseWorker()
	defer getTop5.CloseWorker()

	outputQueue, _ := ch.QueueDeclare(
		"output_consumer", // name
		false,             // durable
		true,              // delete when unused
		true,              // exclusive
		false,             // no-wait
		nil,               // arguments
	)

	_ = ch.QueueBind(
		outputQueue.Name,   // queue name
		"",                 // routing key
		"get_top_5_output", // exchange
		false,              // no-wait
		nil,                // arguments
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

	go filterOneCountry.RunWorker()
	time.Sleep(2 * time.Second)

	go groupByCountry.RunWorker()
	time.Sleep(2 * time.Second)

	go getTop5.RunWorker()
	time.Sleep(2 * time.Second)

	for msg := range msgs {
		println("Received:", string(msg.Body))
		f, _ := os.OpenFile("test_query2.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		f.WriteString(string(msg.Body) + "\n")
	}

	ch.ExchangeDelete("filter_only_one_output_exchange", false, false)
	println("Exchanges deleted")
}

func main() {
	run_test_for_query_1()
	//run_test_for_query_2()
}
