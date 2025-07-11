package top_ten_cast_movie

import (
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type TopTenCastMovieConfig struct {
	worker.WorkerConfig
}

type TopTenCastMovie struct {
	worker.Worker
	top_ten          map[string]map[string][]TopTenCastCount
	storage_base_dir string
	eof_id           map[string]string
	node_id          map[string]string
}

type TopTenCastCount struct {
	Actor string
	Count int
}

var log = logging.MustGetLogger("top_ten_cast_movie")

func (g *TopTenCastMovie) EnsureClient(client_id string) {
	if _, ok := g.top_ten[client_id]; !ok {
		g.top_ten[client_id] = make(map[string][]TopTenCastCount, 0)
	}
	if _, ok := g.top_ten[client_id][client_id]; !ok {
		g.top_ten[client_id][client_id] = make([]TopTenCastCount, 0)
	}

	if _, ok := g.eof_id[client_id]; !ok {
		ids_to_append := make([]string, 2)
		message_id, err := uuid.NewRandom()
		if err != nil {
			log.Errorf("Error generating uuid: %s", err.Error())
			return
		}
		ids_to_append[0] = message_id.String()
		g.eof_id[client_id] = message_id.String()
		message_id, err = uuid.NewRandom()
		if err != nil {
			log.Errorf("Error generating uuid: %s", err.Error())
			return
		}
		ids_to_append[1] = message_id.String()
		g.node_id[client_id] = message_id.String()
		common_statefull_worker.StoreMyId(g.storage_base_dir, ids_to_append, client_id)
	}
}

func (g *TopTenCastMovie) HandleCommit(client_id string, message amqp091.Delivery) error {
	err := common_statefull_worker.StoreElements(g.top_ten[client_id], client_id, g.storage_base_dir)
	if err != nil {
		return err
	}
	message.Ack(false)
	return nil
}

func (g *TopTenCastMovie) MapToLines(client_id string) string {
	return mapToLines(g.top_ten[client_id][client_id])
}

func mapToLines(top_ten []TopTenCastCount) string {
	var lines []string
	for _, actor_in_top := range top_ten {
		line := fmt.Sprintf("%s%s%d", actor_in_top.Actor, worker.MESSAGE_SEPARATOR, actor_in_top.Count)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (g *TopTenCastMovie) HandleEOF(client_id string, message_id string) error {
	err := common_statefull_worker.SendResult(g.Worker, client_id, g.MapToLines(client_id), g.node_id[client_id], g.eof_id[client_id])
	if err != nil {
		return err
	}
	delete(g.top_ten, client_id)
	common_statefull_worker.CleanTopNNode(g.storage_base_dir, client_id)
	return nil
}

func (g *TopTenCastMovie) UpdateState(lines []string, client_id string, message_id string) bool {
	g.top_ten[client_id][client_id] = updateTopTen(lines, g.top_ten[client_id][client_id])
	return false
}

// ---------------------------------
// MESSAGE FORMAT: ACTOR|COUNT
// ---------------------------------
const ACTOR = 0
const COUNT = 1

// Updates the top ten actors. if we received an element that is already in the top,
// we aren't going to store it as a top needs different elements
func updateTopTen(lines []string, top_ten []TopTenCastCount) []TopTenCastCount {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		actor := parts[ACTOR]
		count, err := strconv.Atoi(parts[COUNT])
		if err != nil {
			continue
		}
		new_actor := TopTenCastCount{Actor: actor, Count: count}
		if slices.Contains(top_ten, new_actor) {
			continue
		}

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

func NewTopTenCastMovie(config TopTenCastMovieConfig, storage_base_dir string) *TopTenCastMovie {
	log.Infof("TopTenCastMovie: %+v", config)
	grouped_elements, _ := common_statefull_worker.GetElements[[]TopTenCastCount](storage_base_dir)

	my_id, _ := common_statefull_worker.GetMyId(storage_base_dir)
	eof_id := make(map[string]string)
	node_id := make(map[string]string)
	for key, val := range my_id {
		if len(val) == 2 {
			eof_id[key] = val[0]
			node_id[key] = val[1]
		}
	}

	worker, err := worker.NewWorker(config.WorkerConfig, 1)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &TopTenCastMovie{
		Worker:           *worker,
		top_ten:          grouped_elements,
		storage_base_dir: storage_base_dir,
		eof_id:           eof_id,
		node_id:          node_id,
	}
}
