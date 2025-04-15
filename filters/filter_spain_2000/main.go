package filter_spain_2000

import (
	"distribuidos-tp1/common/worker"
)

func main() {
	filter := NewFilterBySpainAndOf2000(FilterBySpainAndOf2000Config{
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
