package filter_only_one_country

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/filters/common_filter"
	"strconv"
	"strings"
)

type FilterByOnlyOneCountryConfig struct {
	worker.WorkerConfig
}

type FilterByOnlyOneCountry struct {
	*common_filter.CommonFilter
}

func NewFilterByOnlyOneCountry(config FilterByOnlyOneCountryConfig) *FilterByOnlyOneCountry {
	filter := common_filter.NewCommonFilter(config.WorkerConfig)
	return &FilterByOnlyOneCountry{
		CommonFilter: filter,
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|...
// ---------------------------------
const COUNTRIES = 3

func (f *FilterByOnlyOneCountry) Filter(lines []string) bool {
	anyMoviesFound := false
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_id, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		countries := strings.Split(parts[COUNTRIES], worker.MESSAGE_ARRAY_SEPARATOR)
		if len(countries) == 1 {
			f.Buffer.AddMessage(movie_id, strings.TrimSpace(line))
			anyMoviesFound = true
		}
	}
	return anyMoviesFound
}

func (f *FilterByOnlyOneCountry) HandleEOF(client_id string, message_id string) error {
	return f.CommonFilter.HandleEOF(client_id, message_id)
}

func (f *FilterByOnlyOneCountry) SendMessage(client_id string, message_id string) error {
	return f.CommonFilter.SendMessage(client_id, message_id)
}

func (f *FilterByOnlyOneCountry) CloseWorker() {
	f.CommonFilter.CloseWorker()
}
