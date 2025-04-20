package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	"fmt"

	cmd "distribuidos-tp1/filters/cmd"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("filter")

func main() {
	// Uncomment the following lines if you want to load environment variables from a .env file without using docker
	// err := godotenv.Load()
	// if err != nil {
	// 	log.Errorf("Error loading .env file")
	// }
	v := viper.New()
	v.AutomaticEnv()

	log_level := v.GetString("LOG_LEVEL")
	filterName := v.GetString("FILTER_NAME")
	inputExchange := v.GetString("WORKER_EXCHANGE_INPUT")
	outputExchange := v.GetString("WORKER_EXCHANGE_OUTPUT")
	messageBroker := v.GetString("WORKER_BROKER")

	fmt.Printf("LOG_LEVEL: %s\n", log_level)
	fmt.Printf("FILTER_NAME: %s\n", filterName)
	fmt.Printf("WORKER_EXCHANGE_INPUT: %s\n", inputExchange)
	fmt.Printf("WORKER_EXCHANGE_OUTPUT: %s\n", outputExchange)
	fmt.Printf("WORKER_BROKER: %s\n", messageBroker)

	if filterName == "" || inputExchange == "" || outputExchange == "" || messageBroker == "" {
		log.Criticalf("Error: one or more environment variables are empty")
		return
	}

	if err := utils.InitLogger(log_level); err != nil {
		log.Criticalf("%s", err)
		return
	}

	filter, err := cmd.InitializeFilter(filterName, worker.WorkerConfig{
		InputExchange:  inputExchange,
		OutputExchange: outputExchange,
		MessageBroker:  messageBroker,
	})
	if err != nil {
		log.Criticalf("Error initializing filter: %s", err)
		return
	}

	defer filter.CloseWorker()

	err = filter.RunWorker()
	if err != nil {
		panic(err)
	}
}
