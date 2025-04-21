package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"fmt"

	filter "distribuidos-tp1/filters/filter_after_2000"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("filter_after_2000")

func main() {
	// Uncomment the following lines if you want to load environment variables from a .env file without using docker
	// err := godotenv.Load()
	// if err != nil {
	// 	log.Errorf("Error loading .env file")
	// }
	v := viper.New()
	v.AutomaticEnv()

	log_level := v.GetString("LOG_LEVEL")
	inputExchange := v.GetString("WORKER_EXCHANGE_INPUT")
	outputExchange := v.GetString("WORKER_EXCHANGE_OUTPUT")
	messageBroker := v.GetString("WORKER_BROKER")

	fmt.Printf("LOG_LEVEL: %s\n", log_level)
	fmt.Printf("WORKER_EXCHANGE_INPUT: %s\n", inputExchange)
	fmt.Printf("WORKER_EXCHANGE_OUTPUT: %s\n", outputExchange)
	fmt.Printf("WORKER_BROKER: %s\n", messageBroker)

	if inputExchange == "" || outputExchange == "" || messageBroker == "" {
		log.Criticalf("Error: one or more environment variables are empty")
		return
	}

	if err := utils.InitLogger(log_level); err != nil {
		log.Criticalf("%s", err)
		return
	}

	filter := filter.NewFilterByAfterYear2000(filter.FilterByAfterYear2000Config{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  v.GetString("worker.exchange.input"),
			OutputExchange: v.GetString("worker.exchange.output"),
			MessageBroker:  v.GetString("worker.broker"),
		},
	})

	defer filter.CloseWorker()

	err := filter.RunWorker()
	if err != nil {
		panic(err)
	}
}
