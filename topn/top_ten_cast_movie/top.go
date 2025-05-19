package top_ten_cast_movie

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type TopTenCastMovieConfig struct {
	worker.WorkerConfig
}

type TopTenCastMovie struct {
	worker.Worker
	top_ten                map[string][]TopTenCastCount
	messages_before_commit int
}

type TopTenCastCount struct {
	Actor string
	Count int
}

var log = logging.MustGetLogger("top_ten_cast_movie")

func (g *TopTenCastMovie) NewClient(client_id string) {
	if _, ok := g.top_ten[client_id]; !ok {
		g.top_ten[client_id] = make([]TopTenCastCount, 0)
	}
}

func (g *TopTenCastMovie) ShouldCommit(messages_before_commit int, client_id string) bool {
	if messages_before_commit >= g.messages_before_commit {
		storeGroupedElements(g.top_ten[client_id], client_id)
		return true
	}
	return false
}

func (g *TopTenCastMovie) MapToLines(client_id string) string {
	return mapToLines(g.top_ten[client_id])
}

func mapToLines(top_ten []TopTenCastCount) string {
	var lines []string
	for _, actor_in_top := range top_ten {
		line := fmt.Sprintf("%s%s%d", actor_in_top.Actor, worker.MESSAGE_SEPARATOR, actor_in_top.Count)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *TopTenCastMovie) HandleEOF(client_id string) error {
	err := common_statefull_worker.SendResult(g.Worker, g, client_id)
	if err != nil {
		return err
	}
	delete(g.top_ten, client_id)
	return nil
}

func (g *TopTenCastMovie) UpdateState(lines []string, client_id string) {
	g.top_ten[client_id] = updateTopTen(lines, g.top_ten[client_id])
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
		new_actor := TopTenCastCount{Actor: actor, Count: count}

		if len(top_ten) < 10 {
			top_ten = append(top_ten, new_actor)
		} else {
			last := top_ten[9]
			if count > last.Count || (count == last.Count && actor < last.Actor) {
				top_ten[9] = new_actor
			} else {
				continue
			}
		}

		sort.Slice(top_ten, func(i, j int) bool {
			if top_ten[i].Count != top_ten[j].Count {
				return top_ten[i].Count > top_ten[j].Count
			}
			return top_ten[i].Actor < top_ten[j].Actor
		})

	}
	return top_ten
}

func storeGroupedElements(results []TopTenCastCount, client_id string) {
	// TODO: Dumpear el hashmap a un archivo
}

func getGroupedElements() []TopTenCastCount {
	// TODO: Cuando se caiga un worker, deberia leer de este archivo lo que estuvo obteniendo
	return []TopTenCastCount{}
}

func NewTopTenCastMovie(config TopTenCastMovieConfig, messages_before_commit int) *TopTenCastMovie {
	log.Infof("TopTenCastMovie: %+v", config)
	return &TopTenCastMovie{
		Worker: worker.Worker{
			InputExchange:  config.InputExchange,
			OutputExchange: config.OutputExchange,
			MessageBroker:  config.MessageBroker,
		},
		top_ten:                make(map[string][]TopTenCastCount, 0),
		messages_before_commit: messages_before_commit,
	}
}

func (g *TopTenCastMovie) RunWorker(starting_message string) error {
	msgs, err := common_statefull_worker.Init(&g.Worker, starting_message)
	if err != nil {
		return err
	}
	return common_statefull_worker.RunWorker(g, msgs)
}
