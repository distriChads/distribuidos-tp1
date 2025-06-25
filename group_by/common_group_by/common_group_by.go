package common_group_by

import (
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"slices"

	"github.com/google/uuid"
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
	eof_id                    map[string]string
	node_id                   map[string]string
}

// Init the in memory state for the client if needed and creates the ids for the messages it will send at the end
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

// Appends the message from rabbit that has been received and increase the global counter of messages received
// if we have the number of messages to commit, this is what we do:
// first, for every client we store the state with the last messages_ids and appends the ids
// in the file we store the last_messages. this is important because if we die during the append, we will restore
// the appends messages for repeated with this information.
// if everything went fine, we now have the state and the append file ready, so we can ack all messages that are stored and reset the counter
// if we die during the .Ack, we now have everything in state and we will simply receive some repeated messages that we are going to skip
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

// Verify if the messages is repeated, if not append it to the messages_id for that client
func (g *CommonGroupBy[T]) VerifyRepeatedMessage(client_id string, message_id string) bool {
	if slices.Contains(g.messages_id[client_id], message_id) {
		log.Warning("MENSAJE REPETIDO")
		return true
	}
	g.messages_id[client_id] = append(g.messages_id[client_id], message_id)
	return false
}

// Handle a received EOF message. if we already received it we just skip it
// if it's a new eof we do this:
// first, we append it to the eofs in memory state to know if we receive it again
// we then store this eofs, our inner state and ack all messages of that client
// if getting the ack makes us getting the expected amount, we first store our inner state
// then we send the result to the next node, we ack every message and clean our state
// if we die a line before storing our state, there is a problem if the prefetch isn't 1.
// Rabbit does not ensure the order of the messages when the node dies, so we can have this
// message - eof - message. and we will be skipping this message, so becareful when the env var of maxmessages != 1
// if we die after storing and before ack the messages, we will receive then again and we will discard them
// if we died after sending the results, we will not ack the eof, but this is no problem, we will receive it again
// we will store what we have again and send the message. because we have the node_ids, the message will be with the same id
// so the next node will discard it as a repeated
// if everything goes well, we just clean our state at the end
func (g *CommonGroupBy[T]) HandleEOF(client_id string, message_id string, lines string) error {
	if slices.Contains(g.eofs[client_id][client_id], message_id) {
		log.Warning("EOF REPETIDO")
		return nil
	}
	g.eofs[client_id][client_id] = append(g.eofs[client_id][client_id], message_id)
	if len(g.eofs[client_id][client_id]) >= g.expected_eof {
		err := common_statefull_worker.StoreElementsWithMessageIds(g.Grouped_elements[client_id], client_id, g.storage_base_dir, []string{message_id})
		if err != nil {
			return err
		}
		for _, message := range g.messages[client_id] {
			message.Ack(false)
		}
		err = common_statefull_worker.SendResult(g.Worker, client_id, lines, g.node_id[client_id], g.eof_id[client_id])
		if err != nil {
			return err
		}
		g.messages[client_id] = g.messages[client_id][:0]
		delete(g.messages, client_id)
		delete(g.Grouped_elements, client_id)
		delete(g.eofs, client_id)
		common_statefull_worker.CleanGroupNode(g.storage_base_dir, client_id)
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

// Creates a new group by, this are the steps
// first, we get the elements from the state, the ids and the eofs
// second we restore the state of the ids if needed (this is because we commited our state, but the append file is incomplete)
// if we had to update, we get this ids again
// then, we get our messages_id for every client and we can start our new worker
func NewCommonGroupBy[T any](config worker.WorkerConfig, messages_before_commit int, storage_base_dir string, expected_eof int) *CommonGroupBy[T] {
	log.Infof("New group by: %+v", config)
	grouped_elements, last_messages_in_state := common_statefull_worker.GetElements[T](storage_base_dir)
	messages_id, last_message_in_id := common_statefull_worker.GetIds(storage_base_dir)
	eofs, _ := common_statefull_worker.GetEofs[[]string](storage_base_dir)
	need_to_update, err := common_statefull_worker.RestoreStateIfNeeded(last_messages_in_state, last_message_in_id, storage_base_dir)
	if err != nil {
		log.Errorf("Error restoring state: %s", err)
		return nil
	}
	if need_to_update {
		messages_id, _ = common_statefull_worker.GetIds(storage_base_dir)
	}

	my_id, _ := common_statefull_worker.GetMyId(storage_base_dir)
	eof_id := make(map[string]string)
	node_id := make(map[string]string)
	for key, val := range my_id {
		if len(val) == 2 {
			eof_id[key] = val[0]
			node_id[key] = val[1]
		}
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
		eof_id:                    eof_id,
		node_id:                   node_id,
	}
}
