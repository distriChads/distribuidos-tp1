package filter_argentina

import (
	"distribuidos-tp1/common/worker"
)

func main() {
	filter := NewFilterByArgentina(FilterByArgentinaConfig{
		WorkerConfig: worker.WorkerConfig{
			InputExchange:  "input_exchange",
			OutputExchange: "output_exchange",
			MessageBroker:  "amqp://guest:guest@localhost:5672/",
		},
	})

	defer filter.CloseWorker()

	err := filter.RunWorker()
	if err != nil {
		panic(err)
	}
}
