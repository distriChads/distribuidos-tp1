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

	filterArgentina := filter_argentina.NewFilterByArgentina(filter_argentina.FilterByArgentinaConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "movies",
				RoutingKeys: []string{"movies.input"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "filter_argentina_output",
				RoutingKeys: []string{"filter.argentina.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	})

	filterSpain := filter_spain_2000.NewFilterBySpainAndOf2000(filter_spain_2000.FilterBySpainAndOf2000Config{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "filter_argentina_output",
				RoutingKeys: []string{"filter.argentina.output"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "filter_spain_output",
				RoutingKeys: []string{"filter.spain.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	})

	defer filterArgentina.CloseWorker()
	defer filterSpain.CloseWorker()

	err = ch.ExchangeDeclare(
		"filter_spain_output", // name
		"topic",               // type
		false,                 // durable
		true,                  // delete when unused
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		println("Error declaring exchange:", err.Error())
	}

	outputQueue, _ := ch.QueueDeclare(
		"q1_test_output", // name
		false,            // durable
		true,             // delete when unused
		true,             // exclusive
		false,            // no-wait
		nil,              // arguments
	)

	err = ch.QueueBind(
		outputQueue.Name,      // queue name
		"filter.spain.output", // routing key
		"filter_spain_output", // exchange
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		println("Error binding queue:", err.Error())
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
		println("Error consuming queue:", err.Error())
	}

	go filterArgentina.RunWorker()
	go filterSpain.RunWorker()

	for msg := range msgs {
		println("======= Received:", string(msg.Body), "=======")
		f, _ := os.OpenFile("test_query1.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		f.WriteString(string(msg.Body) + "\n")
	}
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

	filterOneCountry := filter_only_one_country.NewFilterByOnlyOneCountry(filter_only_one_country.FilterByOnlyOneCountryConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "movies",
				RoutingKeys: []string{"movies.input"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "filter_only_one_country_output",
				RoutingKeys: []string{"filter.only-one-country.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	})

	groupByCountry := group_by_country_sum.NewGroupByCountryAndSum(group_by_country_sum.GroupByCountryAndSumConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "filter_only_one_country_output",
				RoutingKeys: []string{"filter.only-one-country.output"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "group_by_country_output",
				RoutingKeys: []string{"groupby.country.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	getTop5 := top_five_country_budget.NewTopFiveCountryBudget(top_five_country_budget.TopFiveCountryBudgetConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "group_by_country_output",
				RoutingKeys: []string{"groupby.country.output"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "get_top_5_output",
				RoutingKeys: []string{"topn.top-5-country.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
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
		outputQueue.Name,            // queue name
		"topn.top-5-country.output", // routing key
		"get_top_5_output",          // exchange
		false,                       // no-wait
		nil,                         // arguments
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

	filterByArgentina := filter_argentina.NewFilterByArgentina(filter_argentina.FilterByArgentinaConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "movies",
				RoutingKeys: []string{"movies.input"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "filter_by_argentina_output",
				RoutingKeys: []string{"filter.argentina.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	})

	filterAfter2000 := filterafter2000.NewFilterByAfterYear2000(filterafter2000.FilterByAfterYear2000Config{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "filter_by_argentina_output",
				RoutingKeys: []string{"filter.argentina.output"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "filter_after_2000_output",
				RoutingKeys: []string{"filter.after-2000.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	})

	joinMovieRating := join_movie_ratings.NewJoinMovieRatingById(join_movie_ratings.JoinMovieRatingByIdConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "filter_after_2000_output",
				RoutingKeys: []string{"filter.after-2000.output"},
			},
			SecondInputExchange: worker.ExchangeSpec{
				Name:        "ratings",
				RoutingKeys: []string{"ratings.input"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "join_output",
				RoutingKeys: []string{"join.movie-ratings.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	groupByMovie := group_by_movie_avg.NewGroupByMovieAndAvg(group_by_movie_avg.GroupByMovieAndAvgConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "join_output",
				RoutingKeys: []string{"join.movie-ratings.output"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "group_by_output",
				RoutingKeys: []string{"groupby.movie-avg.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	firstAndLast := first_and_last.NewFirstAndLast(first_and_last.FirstAndLastConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "group_by_output",
				RoutingKeys: []string{"groupby.movie-avg.output"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "first_and_last_output",
				RoutingKeys: []string{"topn.first-last.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
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
		outputQueue.Name,         // queue name
		"topn.first-last.output", // routing key
		"first_and_last_output",  // exchange
		false,                    // no-wait
		nil,                      // arguments
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

	filterByArgentina := filter_argentina.NewFilterByArgentina(filter_argentina.FilterByArgentinaConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "movies",
				RoutingKeys: []string{"movies.input"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "filter_by_argentina_output",
				RoutingKeys: []string{"filter.argentina.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	})

	filterAfter2000 := filterafter2000.NewFilterByAfterYear2000(filterafter2000.FilterByAfterYear2000Config{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "filter_by_argentina_output",
				RoutingKeys: []string{"filter.argentina.output"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "filter_after_2000_output",
				RoutingKeys: []string{"filter.after-2000.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	})

	joinMovieCredits := join_movie_credits.NewJoinMovieCreditsById(join_movie_credits.JoinMovieCreditsByIdConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "filter_after_2000_output",
				RoutingKeys: []string{"filter.after-2000.output"},
			},
			SecondInputExchange: worker.ExchangeSpec{
				Name:        "credits",
				RoutingKeys: []string{"credits.input"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "join_output",
				RoutingKeys: []string{"join.movie-credits.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	groupByActor := group_by_actor_count.NewGroupByActorAndCount(group_by_actor_count.GroupByActorAndCountConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "join_output",
				RoutingKeys: []string{"join.movie-credits.output"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "group_by_output",
				RoutingKeys: []string{"groupby.actor.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
		},
	}, 10)

	topTen := top_ten_cast_movie.NewTopTenCastMovie(top_ten_cast_movie.TopTenCastMovieConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange: worker.ExchangeSpec{
				Name:        "group_by_output",
				RoutingKeys: []string{"groupby.actor.output"},
			},
			OutputExchange: worker.ExchangeSpec{
				Name:        "top_10_output",
				RoutingKeys: []string{"topn.top-10-cast.output"},
			},
			MessageBroker: "amqp://guest:guest@localhost:5672/",
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
		outputQueue.Name,          // queue name
		"topn.top-10-cast.output", // routing key
		"top_10_output",           // exchange
		false,                     // no-wait
		nil,                       // arguments
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
	run_test_for_query_1()
	//run_test_for_query_2()
	//run_test_for_query_3()
	//run_test_for_query_4()
}
