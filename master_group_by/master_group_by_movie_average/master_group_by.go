package master_group_by_movie_avg

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/group_by/common_group_by"
	"fmt"
	"strconv"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

type MasterGroupByMovieAndAvgConfig struct {
	worker.WorkerConfig
}

type MasterGroupByMovieAndAvg struct {
	*common_group_by.CommonGroupBy[ScoreAndCount]
}

type ScoreAndCount struct {
	Count int
	Score float64
}

func (g *MasterGroupByMovieAndAvg) EnsureClient(client_id string) {
	g.CommonGroupBy.EnsureClient(client_id)
}

func (g *MasterGroupByMovieAndAvg) HandleCommit(client_id string, message amqp091.Delivery) error {
	return g.CommonGroupBy.HandleCommit(client_id, message)
}

func (g *MasterGroupByMovieAndAvg) MapToLines(client_id string) string {
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

func (g *MasterGroupByMovieAndAvg) HandleEOF(client_id string, message_id string) error {
	return g.CommonGroupBy.HandleEOF(client_id, message_id, g.MapToLines(client_id))
}

func (g *MasterGroupByMovieAndAvg) UpdateState(lines []string, client_id string, message_id string) bool {
	repeated_message := g.CommonGroupBy.VerifyRepeatedMessage(client_id, message_id)
	if !repeated_message {
		groupByMovieAndUpdate(lines, g.CommonGroupBy.Grouped_elements[client_id])
	}
	return repeated_message
}

// ---------------------------------
// MESSAGE FORMAT: TITLE|AVERAGE
// ---------------------------------
const TITLE = 0
const SCORE = 1

// Store the movie as a key and sum the counter and the average received
func groupByMovieAndUpdate(lines []string, grouped_elements map[string]ScoreAndCount) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		average, err := strconv.ParseFloat(parts[SCORE], 64)
		if err != nil {
			continue
		}
		current := grouped_elements[parts[TITLE]]
		current.Score += average
		current.Count += 1
		grouped_elements[parts[TITLE]] = current

	}
}

func NewGroupByMovieAndAvg(config MasterGroupByMovieAndAvgConfig, expected_eof int, storage_base_dir string) *MasterGroupByMovieAndAvg {
	group_by := common_group_by.NewCommonGroupBy[ScoreAndCount](config.WorkerConfig, 1, storage_base_dir, expected_eof)
	return &MasterGroupByMovieAndAvg{
		CommonGroupBy: group_by,
	}
}
