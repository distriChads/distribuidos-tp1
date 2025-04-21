package main

import (
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/group_by_actor_count"
	"distribuidos-tp1/group_by/group_by_country_sum"
	group_by_movie_avg "distribuidos-tp1/group_by/group_by_movie_average"
	"distribuidos-tp1/joins/join_movie_credits"
	"distribuidos-tp1/joins/join_movie_ratings"
	"distribuidos-tp1/topn/first_and_last"
	"distribuidos-tp1/topn/top_five_country_budget"
	"distribuidos-tp1/topn/top_ten_cast_movie"
	"os"

	filterafter2000 "distribuidos-tp1/filters/filter_after_2000"
	"distribuidos-tp1/filters/filter_argentina"
	"distribuidos-tp1/filters/filter_only_one_country"
	"distribuidos-tp1/filters/filter_spain_2000"

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
	go filterSpain.RunWorker()

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

	go groupByCountry.RunWorker()

	go getTop5.RunWorker()

	for msg := range msgs {
		println("Received:", string(msg.Body))
		f, _ := os.OpenFile("test_query2.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		f.WriteString(string(msg.Body) + "\n")
	}

	ch.ExchangeDelete("filter_only_one_output_exchange", false, false)
	println("Exchanges deleted")
}

func run_test_for_query_3() {
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

	err = ch.ExchangeDeclare(
		"ratings", // name
		"fanout",  // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"filter_by_argentina_output", // name
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

	err = ch.ExchangeDeclare(
		"filter_after_2000_output", // name
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
		"join_output", // name
		"fanout",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"group_by_output", // name
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

	err = ch.ExchangeDeclare(
		"first_and_last_output", // name
		"fanout",                // type
		true,                    // durable
		false,                   // auto-deleted
		false,                   // internal
		false,                   // no-wait
		nil,                     // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	filterByArgentina := filter_argentina.NewFilterByArgentina(filter_argentina.FilterByArgentinaConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "movies",
			OutputExchange: "filter_by_argentina_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	filterAfter2000 := filterafter2000.NewFilterByAfterYear2000(filterafter2000.FilterByAfterYear2000Config{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "filter_by_argentina_output",
			OutputExchange: "filter_after_2000_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	joinMovieRating := join_movie_ratings.NewJoinMovieRatingById(join_movie_ratings.JoinMovieRatingByIdConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:       "filter_after_2000_output",
			SecondInputExchange: "ratings",
			OutputExchange:      "join_output",
			MessageBroker:       "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	groupByMovie := group_by_movie_avg.NewGroupByMovieAndAvg(group_by_movie_avg.GroupByMovieAndAvgConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "join_output",
			OutputExchange: "group_by_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	firstAndLast := first_and_last.NewFirstAndLast(first_and_last.FirstAndLastConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "group_by_output",
			OutputExchange: "first_and_last_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	defer filterByArgentina.CloseWorker()
	defer filterAfter2000.CloseWorker()
	defer joinMovieRating.CloseWorker()
	defer groupByMovie.CloseWorker()
	defer firstAndLast.CloseWorker()

	outputQueue, _ := ch.QueueDeclare(
		"output_consumer", // name
		false,             // durable
		true,              // delete when unused
		true,              // exclusive
		false,             // no-wait
		nil,               // arguments
	)

	_ = ch.QueueBind(
		outputQueue.Name,        // queue name
		"",                      // routing key
		"first_and_last_output", // exchange
		false,                   // no-wait
		nil,                     // arguments
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

	go filterByArgentina.RunWorker()
	go filterAfter2000.RunWorker()
	go joinMovieRating.RunWorker()
	go groupByMovie.RunWorker()
	go firstAndLast.RunWorker()

	for msg := range msgs {
		println("Received:", string(msg.Body))
		f, _ := os.OpenFile("test_query3.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		f.WriteString(string(msg.Body) + "\n")
	}

}

func run_test_for_query_4() {
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

	err = ch.ExchangeDeclare(
		"credits", // name
		"fanout",  // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"filter_by_argentina_output", // name
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

	err = ch.ExchangeDeclare(
		"filter_after_2000_output", // name
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
		"join_output", // name
		"fanout",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	err = ch.ExchangeDeclare(
		"group_by_output", // name
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

	err = ch.ExchangeDeclare(
		"top_10_output", // name
		"fanout",        // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		println(err.Error())
		return
	}

	filterByArgentina := filter_argentina.NewFilterByArgentina(filter_argentina.FilterByArgentinaConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "movies",
			OutputExchange: "filter_by_argentina_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	filterAfter2000 := filterafter2000.NewFilterByAfterYear2000(filterafter2000.FilterByAfterYear2000Config{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "filter_by_argentina_output",
			OutputExchange: "filter_after_2000_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	joinMovieCredits := join_movie_credits.NewJoinMovieCreditsById(join_movie_credits.JoinMovieCreditsByIdConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:       "filter_after_2000_output",
			SecondInputExchange: "credits",
			OutputExchange:      "join_output",
			MessageBroker:       "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	groupByActor := group_by_actor_count.NewGroupByActorAndCount(group_by_actor_count.GroupByActorAndCountConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "join_output",
			OutputExchange: "group_by_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	topTen := top_ten_cast_movie.NewTopTenCastMovie(top_ten_cast_movie.TopTenCastMovieConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "group_by_output",
			OutputExchange: "top_10_output",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	defer filterByArgentina.CloseWorker()
	defer filterAfter2000.CloseWorker()
	defer joinMovieCredits.CloseWorker()
	defer groupByActor.CloseWorker()
	defer topTen.CloseWorker()

	outputQueue, _ := ch.QueueDeclare(
		"output_consumer", // name
		false,             // durable
		true,              // delete when unused
		true,              // exclusive
		false,             // no-wait
		nil,               // arguments
	)

	_ = ch.QueueBind(
		outputQueue.Name, // queue name
		"",               // routing key
		"top_10_output",  // exchange
		false,            // no-wait
		nil,              // arguments
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

	go filterByArgentina.RunWorker()
	go filterAfter2000.RunWorker()
	go joinMovieCredits.RunWorker()
	go groupByActor.RunWorker()
	go topTen.RunWorker()

	for msg := range msgs {
		println("Received:", string(msg.Body))
		f, _ := os.OpenFile("test_query4.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		f.WriteString(string(msg.Body) + "\n")
	}

}

func main() {
	//run_test_for_query_1()
	//run_test_for_query_2()
	//run_test_for_query_3()
	run_test_for_query_4()
}
