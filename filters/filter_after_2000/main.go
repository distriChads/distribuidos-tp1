package filter_after_2000

import (
	"distribuidos-tp1/common/utils"
	"distribuidos-tp1/common/worker"
)

func main() {
	v, err := utils.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := utils.InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	utils.PrintConfig(v)

	filter := NewFilterByAfterYear2000(FilterByAfterYear2000Config{
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
