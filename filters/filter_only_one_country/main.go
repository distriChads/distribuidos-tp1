package filter_only_one_country

import (
	"distribuidos-tp1/common/worker"
)

func main() {
	filter := NewFilterByOnlyOneCountry(FilterByOnlyOneCountryConfig{
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
