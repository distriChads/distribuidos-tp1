package filterafter2000

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/filters/common_filter"
	"strconv"
	"strings"
)

type FilterByAfterYear2000Config struct {
	worker.WorkerConfig
}

type FilterByAfterYear2000 struct {
	*common_filter.CommonFilter
}

func NewFilterByAfterYear2000(config FilterByAfterYear2000Config) *FilterByAfterYear2000 {
	filter := common_filter.NewCommonFilter(config.WorkerConfig)
	return &FilterByAfterYear2000{
		CommonFilter: filter,
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|...
// ---------------------------------
const ID = 0
const TITLE = 1
const DATE = 2

func (f *FilterByAfterYear2000) Filter(lines []string) bool {
	anyMoviesFound := false
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_id, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		raw_year := strings.Split(parts[DATE], "-")[0]
		year, err := strconv.Atoi(raw_year)
		if err != nil {
			continue
		}
		if year >= 2000 {
			f.Buffer.AddMessage(movie_id, strings.TrimSpace(parts[ID])+worker.MESSAGE_SEPARATOR+strings.TrimSpace(parts[TITLE]))
			anyMoviesFound = true
		}
	}
	return anyMoviesFound
}

func (f *FilterByAfterYear2000) HandleEOF(client_id string, message_id string) error {
	return f.CommonFilter.HandleEOF(client_id, message_id)
}

func (f *FilterByAfterYear2000) SendMessage(client_id string, message_id string) error {
	return f.CommonFilter.SendMessage(client_id, message_id)
}

func (f *FilterByAfterYear2000) CloseWorker() {
	f.CommonFilter.CloseWorker()
}
