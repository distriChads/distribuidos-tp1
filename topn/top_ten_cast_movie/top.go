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
	top_ten map[string][]TopTenCastCount
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
		top_ten: make(map[string][]TopTenCastCount),
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
	for message := range msgs {
		client_id := strings.Split(message.RoutingKey, ".")[0]
		if _, ok := f.top_ten[client_id]; !ok {
			f.top_ten[client_id] = make([]TopTenCastCount, 0)
		}
		message_str := string(message.Body)
		log.Debugf("Received message: %s", message_str)
		if message_str == worker.MESSAGE_EOF {
			log.Infof("Sending result for client %s", client_id)
			sendResult(f, client_id)
			delete(f.top_ten, client_id)
			log.Infof("Client %s finished", client_id)
			message.Ack(false)
			continue
		}
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		f.top_ten[client_id] = updateTopTen(lines, f.top_ten[client_id])
		message.Ack(false)
	}

	return nil
}

func sendResult(f *TopTenCastMovie, client_id string) error {
	message_to_send := mapToLines(f.top_ten[client_id])
	send_queue_key := client_id + "." + f.Worker.OutputExchange.RoutingKeys[0] // POR QUE VA A ENVIAR A UN UNICO NODO MAESTRO
	err := worker.SendMessage(f.Worker, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	err = worker.SendMessage(f.Worker, worker.MESSAGE_EOF, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	return nil
}
