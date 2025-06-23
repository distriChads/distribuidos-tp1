package common_group_by

import (
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"slices"

	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("common_group_by")

type CommonGroupBy[T any] struct {
	worker.Worker
	messages_before_commit    int
	Grouped_elements          map[string]map[string]T
	eofs                      map[string]map[string][]string
	expected_eof              int
	storage_base_dir          string
	messages_id               map[string][]string
	messages                  map[string][]amqp091.Delivery
	received_messages_counter int
}

func (g *CommonGroupBy[T]) EnsureClient(client_id string) {
	if _, ok := g.Grouped_elements[client_id]; !ok {
		g.Grouped_elements[client_id] = make(map[string]T)
	}
	if _, ok := g.eofs[client_id]; !ok {
		g.eofs[client_id] = make(map[string][]string)
	}
	if _, ok := g.eofs[client_id][client_id]; !ok {
		g.eofs[client_id][client_id] = make([]string, 0)
	}
}

func (g *CommonGroupBy[T]) HandleCommit(client_id string, message amqp091.Delivery) error {
	g.messages[client_id] = append(g.messages[client_id], message)
	g.received_messages_counter++
	if g.received_messages_counter >= g.messages_before_commit {
		for client_id, element := range g.Grouped_elements {
			err := common_statefull_worker.StoreElementsWithMessageIds(element,
				client_id, g.storage_base_dir,
				g.messages_id[client_id][len(g.messages_id[client_id])-len(g.messages[client_id]):])

			if err != nil {
				return err
			}
		}

		for client_id, message_array := range g.messages {
			for _, message := range message_array {
				message.Ack(false)
			}
			g.messages[client_id] = g.messages[client_id][:0]
		}
		g.received_messages_counter = 0

	}
	return nil
}

func (g *CommonGroupBy[T]) VerifyRepeatedMessage(client_id string, message_id string) bool {
	if slices.Contains(g.messages_id[client_id], message_id) {
		log.Warning("MENSAJE REPETIDO")
		return true
	}
	g.messages_id[client_id] = append(g.messages_id[client_id], message_id)
	return false
}

func (g *CommonGroupBy[T]) HandleEOF(client_id string, message_id string, lines string) error {
	if slices.Contains(g.eofs[client_id][client_id], message_id) {
		return nil
	}
	g.eofs[client_id][client_id] = append(g.eofs[client_id][client_id], message_id)
	if len(g.eofs[client_id][client_id]) >= g.expected_eof {
		log.Warning("MOMENTO DE ENVIAR FLACO")
		err := common_statefull_worker.SendResult(g.Worker, client_id, lines)
		if err != nil {
			return err
		}
		err = common_statefull_worker.StoreEofsWithId(g.eofs[client_id], client_id, g.storage_base_dir)
		if err != nil {
			return err
		}
		err = common_statefull_worker.StoreElementsWithMessageIds(g.Grouped_elements[client_id], client_id, g.storage_base_dir, []string{message_id})
		if err != nil {
			return err
		}
		for _, message := range g.messages[client_id] {
			message.Ack(false)
		}
		g.messages[client_id] = g.messages[client_id][:0]
		delete(g.messages, client_id)
		delete(g.Grouped_elements, client_id)
		delete(g.eofs, client_id)
		common_statefull_worker.CleanState(g.storage_base_dir, client_id)
		return nil
	}

	err := common_statefull_worker.StoreEofsWithId(g.eofs[client_id], client_id, g.storage_base_dir)
	if err != nil {
		return err
	}
	err = common_statefull_worker.StoreElementsWithMessageIds(g.Grouped_elements[client_id], client_id, g.storage_base_dir, []string{message_id})
	if err != nil {
		return err
	}
	for _, message := range g.messages[client_id] {
		message.Ack(false)
	}
	g.messages[client_id] = g.messages[client_id][:0]

	return nil
}

func NewCommonGroupBy[T any](config worker.WorkerConfig, messages_before_commit int, storage_base_dir string, expected_eof int) *CommonGroupBy[T] {
	log.Infof("New group by: %+v", config)
	grouped_elements, _, last_messages_in_state := common_statefull_worker.GetElements[T](storage_base_dir)
	messages_id, last_message_in_id := common_statefull_worker.GetIds(storage_base_dir)
	eofs, _, _ := common_statefull_worker.GetEofs[[]string](storage_base_dir)
	need_to_update, err := common_statefull_worker.RestoreStateIfNeeded(last_messages_in_state, last_message_in_id, storage_base_dir)
	if err != nil {
		log.Errorf("Error restoring state: %s", err)
		return nil
	}
	if need_to_update {
		messages_id, _ = common_statefull_worker.GetIds(storage_base_dir)
	}
	worker, err := worker.NewWorker(config, messages_before_commit)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}
	return &CommonGroupBy[T]{
		Worker:                    *worker,
		messages_before_commit:    messages_before_commit,
		Grouped_elements:          grouped_elements,
		eofs:                      eofs,
		expected_eof:              expected_eof,
		storage_base_dir:          storage_base_dir,
		messages_id:               messages_id,
		messages:                  make(map[string][]amqp091.Delivery),
		received_messages_counter: 0,
	}
}
