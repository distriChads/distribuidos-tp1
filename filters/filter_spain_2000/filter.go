package filter_spain_2000

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/filters/common_filter"
	"strconv"
	"strings"
)

type FilterBySpainAndOf2000Config struct {
	worker.WorkerConfig
}

type FilterBySpainAndOf2000 struct {
	*common_filter.CommonFilter
}

func NewFilterBySpainAndOf2000(config FilterBySpainAndOf2000Config) *FilterBySpainAndOf2000 {
	filter := common_filter.NewCommonFilter(config.WorkerConfig)
	return &FilterBySpainAndOf2000{
		CommonFilter: filter,
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|DATE|COUNTRIES|GENRES|...
// ---------------------------------
const ID = 0
const TITLE = 1
const DATE = 2
const COUNTRIES = 3
const GENRES = 4

// Get the countries and the year from the message,
// if spain is in the list and the year is between 2000 and 2009 (inclusive), add to hasher buffer, if not ignore it
func (f *FilterBySpainAndOf2000) Filter(lines []string) bool {
	anyMoviesFound := false
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_id, err := strconv.Atoi(parts[ID])
		if err != nil {
			continue
		}
		raw_year := strings.Split(parts[DATE], "-")[0]
		year, err := strconv.Atoi(raw_year)
		if err != nil {
			continue
		}
		if !(year >= 2000 && year < 2010) {
			continue
		}
		countries := strings.SplitSeq(parts[COUNTRIES], worker.MESSAGE_ARRAY_SEPARATOR)
		for country := range countries {
			if strings.TrimSpace(country) == "ES" {
				f.Buffer.AddMessage(movie_id, strings.TrimSpace(parts[TITLE])+worker.MESSAGE_SEPARATOR+strings.TrimSpace(parts[GENRES]))
				anyMoviesFound = true
				break
			}
		}

	}
	return anyMoviesFound
}

func (f *FilterBySpainAndOf2000) HandleEOF(client_id string, message_id string) error {
	return f.CommonFilter.HandleEOF(client_id, message_id)
}

func (f *FilterBySpainAndOf2000) SendMessage(client_id string, message_id string) error {
	return f.CommonFilter.SendMessage(client_id, message_id)
}

func (f *FilterBySpainAndOf2000) CloseWorker() {
	f.CommonFilter.CloseWorker()
}
