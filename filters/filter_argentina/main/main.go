package main

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker/worker"
	filter "distribuidos-tp1/filters/filter_argentina"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_argentina")

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := utils.InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	utils.PrintConfig(v)

	filter := filter.NewFilterByArgentina(filter.FilterByArgentinaConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  v.GetString("worker.exchange.input"),
			OutputExchange: v.GetString("worker.exchange.output"),
			MessageBroker:  v.GetString("worker.broker"),
		},
	})

	defer filter.CloseWorker()

	err = filter.RunWorker()
	if err != nil {
		panic(err)
	}
}
