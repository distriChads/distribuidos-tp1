package filter_argentina

import (
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/filters/common_filter"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter_argentina")

type FilterByArgentinaConfig struct {
	worker.WorkerConfig
}

type FilterByArgentina struct {
	*common_filter.CommonFilter
}

func NewFilterByArgentina(config FilterByArgentinaConfig) *FilterByArgentina {
	filter := common_filter.NewCommonFilter(config.WorkerConfig)
	return &FilterByArgentina{
		CommonFilter: filter,
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|...
// ---------------------------------
const COUNTRIES = 3

func (f *FilterByArgentina) Filter(lines []string) bool {
	argMovie := false
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_id, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		countries := strings.SplitSeq(parts[COUNTRIES], worker.MESSAGE_ARRAY_SEPARATOR)
		for country := range countries {
			if strings.TrimSpace(country) == "AR" {
				f.Buffer.AddMessage(movie_id, strings.TrimSpace(line))
				argMovie = true
				break
			}
		}
	}
	return argMovie
}

func (f *FilterByArgentina) HandleEOF(client_id string, message_id string) error {
	for _, output_routing_keys := range f.Worker.Exchange.OutputRoutingKeys {
		for _, output_key := range output_routing_keys {
			message := client_id + worker.MESSAGE_SEPARATOR + (message_id + output_key) + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
			err := f.Worker.SendMessage(message, output_key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FilterByArgentina) SendMessage(client_id string, message_id string) error {
	return f.CommonFilter.SendMessage(client_id, message_id)
}

func (f *FilterByArgentina) CloseWorker() {
	f.CommonFilter.CloseWorker()
}
