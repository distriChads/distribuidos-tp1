package group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strconv"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

type GroupByMovieAndAvgConfig struct {
	worker.WorkerConfig
}

type GroupByMovieAndAvg struct {
	*common_group_by.CommonGroupBy[ScoreAndCount]
}

type ScoreAndCount struct {
	Count int
	Score float64
}

func (g *GroupByMovieAndAvg) EnsureClient(client_id string) {
	g.CommonGroupBy.EnsureClient(client_id)
}

func (g *GroupByMovieAndAvg) HandleCommit(client_id string, message amqp091.Delivery) error {
	return g.CommonGroupBy.HandleCommit(client_id, message)
}

func (g *GroupByMovieAndAvg) MapToLines(client_id string) string {
	return mapToLines(g.CommonGroupBy.Grouped_elements[client_id])
}

func mapToLines(grouped_elements map[string]ScoreAndCount) string {
	var lines []string
	for title, value := range grouped_elements {
		average := value.Score / float64(value.Count)
		line := fmt.Sprintf("%s%s%f", title, worker.MESSAGE_SEPARATOR, average)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *GroupByMovieAndAvg) HandleEOF(client_id string, message_id string) error {
	return g.CommonGroupBy.HandleEOF(client_id, message_id, g.MapToLines(client_id))
}

func (g *GroupByMovieAndAvg) UpdateState(lines []string, client_id string, message_id string) {
	if !g.CommonGroupBy.VerifyRepeatedMessage(client_id, message_id) {
		groupByMovieAndUpdate(lines, g.CommonGroupBy.Grouped_elements[client_id])
	}
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE|SCORE
// ---------------------------------
const TITLE = 1
const SCORE = 2

func groupByMovieAndUpdate(lines []string, grouped_elements map[string]ScoreAndCount) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		score, err := strconv.ParseFloat(parts[SCORE], 64)
		if err != nil {
			continue
		}

		current := grouped_elements[parts[TITLE]]
		current.Score += score
		current.Count += 1
		grouped_elements[parts[TITLE]] = current

	}
}

func NewGroupByMovieAndAvg(config GroupByMovieAndAvgConfig, messages_before_commit int, storage_base_dir string, expected_eof int) *GroupByMovieAndAvg {
	group_by := common_group_by.NewCommonGroupBy[ScoreAndCount](config.WorkerConfig, messages_before_commit, storage_base_dir, expected_eof)
	return &GroupByMovieAndAvg{
		CommonGroupBy: group_by,
	}
}
