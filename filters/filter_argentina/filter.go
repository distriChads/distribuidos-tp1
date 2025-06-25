package filter_argentina

import (
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/filters/common_filter"
	"strconv"
	"strings"
)

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
const ID = 0
const COUNTRIES = 3

// Get the countries from the message, if argentina is in the list, add to hasher buffer, if not ignore it
func (f *FilterByArgentina) Filter(lines []string) bool {
	argMovie := false
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_id, err := strconv.Atoi(parts[ID])
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

// This filter is the only one that is behind other filters, that's why it doesn't use the generic handleEOF to send.
// This send the eof the same way, but have to add the output_key to the message_id, because when the other filter nodes receives it
// will duplicate the eof with the same id. Example:
// 1 filter arg, 2 filter spain 2000
// filter arg send ID1|EOF to the 2 filters
// the 2 filters receives ID1|EOF and both of them sends that same message to the group by
// the group by will receive ID1|EOF (nice) and ID1|EOF (repeated message)
// but adding the output_key, we mitigate that problem, as it will be
// ID1FILTER1|EOF, and ID1FILTER2|EOF
// we can't create a new message_id with uuid as it will generate problems with repeated messages, we could have more EOFS flying in the program
// with differents ids
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
