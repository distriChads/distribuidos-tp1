package top_ten_cast_movie

import (
	worker "distribuidos-tp1/common/worker/worker"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("top_ten_cast_movie")

type TopTenCastMovieConfig struct {
	worker.WorkerConfig
}

type TopTenCastMovie struct {
	worker.Worker
}

type TopTenCastCount struct {
	Actor string
	Count int
}

// ---------------------------------
// MESSAGE FORMAT: ACTOR|COUNT
// ---------------------------------
const ACTOR = 0
const COUNT = 1

func updateTopTen(lines []string, top_ten []TopTenCastCount) []TopTenCastCount {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		actor := parts[ACTOR]
		count, err := strconv.Atoi(parts[COUNT])
		if err != nil {
			continue
		}
		if len(top_ten) < 10 {
			top_ten = append(top_ten, TopTenCastCount{Actor: actor, Count: count})
			sort.Slice(top_ten, func(i, j int) bool {
				return top_ten[i].Count > top_ten[j].Count
			})
		} else {
			if top_ten[9].Count < count {
				top_ten[9] = TopTenCastCount{Actor: actor, Count: count}
				sort.Slice(top_ten, func(i, j int) bool {
					if top_ten[i].Count != top_ten[j].Count {
						return top_ten[i].Count > top_ten[j].Count
					}
					return top_ten[i].Actor < top_ten[j].Actor
				})
			}
		}

	}
	return top_ten
}

func mapToLines(top_ten []TopTenCastCount) string {
	var lines []string
	for _, actor_in_top := range top_ten {
		line := fmt.Sprintf("%s%s%d", actor_in_top.Actor, worker.MESSAGE_SEPARATOR, actor_in_top.Count)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func NewTopTenCastMovie(config TopTenCastMovieConfig) *TopTenCastMovie {
	log.Infof("TopTenCastMovie: %+v", config)
	return &TopTenCastMovie{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
	}
}

func (f *TopTenCastMovie) RunWorker() error {
	log.Info("Starting TopTenCastMovie worker")
	worker.InitSender(&f.Worker)
	worker.InitReceiver(&f.Worker)

	msgs, err := worker.ReceivedMessages(f.Worker)
	if err != nil {
		log.Errorf("Error initializing receiver: %s", err.Error())
		return err
	}
	var top_ten []TopTenCastCount
	i := 0
	for message := range msgs {
		i++
		message := string(message.Body)
		log.Info("Batch received Message: ", message)
		if message == worker.MESSAGE_EOF {
			break
		}
		lines := strings.Split(strings.TrimSpace(message), "\n")
		top_ten = updateTopTen(lines, top_ten)
	}
	message_to_send := mapToLines(top_ten)
	log.Infof("Top 10 actors by movie: %s", message_to_send)
	send_queue_key := f.Worker.OutputExchange.RoutingKeys[0] // los topN son nodos unicos, y solo le envian al server
	err = worker.SendMessage(f.Worker, message_to_send, send_queue_key)
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
	}
	return nil
}
