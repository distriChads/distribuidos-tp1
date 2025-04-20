package filters

import (
	"distribuidos-tp1/common/worker/worker"
	after "distribuidos-tp1/filters/filter_after_2000"
	arg "distribuidos-tp1/filters/filter_argentina"
	one_country "distribuidos-tp1/filters/filter_only_one_country"
	spain "distribuidos-tp1/filters/filter_spain_2000"

	"fmt"
)

func InitializeFilter(filterName string, cfg worker.WorkerConfig) (Filter, error) {
	switch filterName {
	case "filter_argentina":
		return arg.NewFilterByArgentina(arg.FilterByArgentinaConfig{WorkerConfig: cfg}), nil
	case "filter_after_2000":
		return after.NewFilterByAfterYear2000(after.FilterByAfterYear2000Config{WorkerConfig: cfg}), nil
	case "filter_only_one_country":
		return one_country.NewFilterByOnlyOneCountry(one_country.FilterByOnlyOneCountryConfig{WorkerConfig: cfg}), nil
	case "filter_spain_2000":
		return spain.NewFilterBySpainAndOf2000(spain.FilterBySpainAndOf2000Config{WorkerConfig: cfg}), nil
	default:
		return nil, fmt.Errorf("unknown filter: %s", filterName)
	}
}
