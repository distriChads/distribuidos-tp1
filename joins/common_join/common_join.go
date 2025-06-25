package common_join

import (
	"context"
	buffer "distribuidos-tp1/common/worker/hasher"
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"errors"
	"slices"
	"strings"

	"github.com/op/go-logging"
)

type CommonJoin struct {
	worker.Worker
	Client_movies_by_id map[string]map[string]string
	Storage_base_dir    string
	Pending             map[string]map[string]string
	Eofs                map[string]map[string][]string
	Expected_eof        int
	Buffer              *buffer.HasherContainer
}

var log = logging.MustGetLogger("common_join")

func NewCommonJoin(config worker.WorkerConfig, storage_base_dir string, eofCounter int) *CommonJoin {
	log.Infof("New join: %+v", config)
	grouped_elements, _ := common_statefull_worker.GetElements[string](storage_base_dir)
	pending, _ := common_statefull_worker.GetPending[string](storage_base_dir)
	eofs, _ := common_statefull_worker.GetEofs[[]string](storage_base_dir)
	worker, err := worker.NewWorker(config, 1)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	dict := make(map[string]int)
	for nodeType, routingKeys := range config.Exchange.OutputRoutingKeys {
		dict[nodeType] = len(routingKeys)
	}
	buffer := buffer.NewHasherContainer(dict)

	return &CommonJoin{
		Worker:              *worker,
		Client_movies_by_id: grouped_elements,
		Eofs:                eofs,
		Expected_eof:        eofCounter,
		Storage_base_dir:    storage_base_dir,
		Pending:             pending,
		Buffer:              buffer,
	}
}

func (f *CommonJoin) HandleJoiningEOF(client_id string, message_id string) error {
	log.Warning("EOF RECEIVED FOR RATINGS")
	for _, output_routing_keys := range f.Worker.Exchange.OutputRoutingKeys {
		for _, output_key := range output_routing_keys {
			message := client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
			err := f.Worker.SendMessage(message, output_key)
			if err != nil {
				return err
			}
		}
	}

	delete(f.Client_movies_by_id, client_id)
	delete(f.Eofs, client_id)
	common_statefull_worker.CleanJoinNode(f.Storage_base_dir, client_id)

	return nil
}

func (f *CommonJoin) HandleMovieEOF(client_id string, message_id string) error {
	log.Warning("RECIBO EOF DE LAS MOVIES")
	if slices.Contains(f.Eofs[client_id][client_id], message_id) {
		log.Warning("EOF REPETIDO")
		return nil
	}
	f.Eofs[client_id][client_id] = append(f.Eofs[client_id][client_id], message_id)
	if len(f.Eofs[client_id][client_id]) >= f.Expected_eof {
		err := common_statefull_worker.StoreEofsWithId(f.Eofs[client_id], client_id, f.Storage_base_dir)
		if err != nil {
			return err
		}
		return nil
	}
	err := common_statefull_worker.StoreEofsWithId(f.Eofs[client_id], client_id, f.Storage_base_dir)
	if err != nil {
		return err
	}
	return nil
}

func (f *CommonJoin) EnsureClient(client_id string) {
	if _, ok := f.Client_movies_by_id[client_id]; !ok {
		f.Client_movies_by_id[client_id] = make(map[string]string)
	}
	if _, ok := f.Eofs[client_id]; !ok {
		f.Eofs[client_id] = make(map[string][]string)
	}
	if _, ok := f.Eofs[client_id][client_id]; !ok {
		f.Eofs[client_id][client_id] = make([]string, 0)
	}
}

func (f *CommonJoin) HandlePending(client_id string, message_id string, message_str string) {
	if _, ok := f.Pending[client_id]; !ok {
		f.Pending[client_id] = make(map[string]string)
	}
	pendings_for_client := f.Pending[client_id]
	pendings_for_client[message_id] = message_str
	common_statefull_worker.StorePending(f.Pending[client_id], client_id, f.Storage_base_dir)

}

func (f *CommonJoin) SendPendings(client_id string, join_function func(lines []string, movies_by_id map[string]string)) error {
	if len(f.Pending[client_id]) != 0 {
		pending_messages := f.Pending[client_id]
		for message_id, pending_message := range pending_messages {
			err := f.HandleLine(client_id, message_id, pending_message, join_function)
			if err != nil {
				return err
			}
		}
		common_statefull_worker.CleanPending(f.Storage_base_dir, client_id)
		delete(f.Pending, client_id)
	}
	return nil
}

func (f *CommonJoin) HandleLine(client_id string, message_id string, line string, join_function func(lines []string, movies_by_id map[string]string)) error {
	if line == worker.MESSAGE_EOF {
		f.HandleJoiningEOF(client_id, message_id)
		return nil
	}
	lines := strings.Split(strings.TrimSpace(line), "\n")
	join_function(lines, f.Client_movies_by_id[client_id])
	for node_type := range f.Worker.Exchange.OutputRoutingKeys {
		messages_to_send := f.Buffer.GetMessages(node_type)
		for routing_key_index, message := range messages_to_send {
			if len(message) != 0 {
				routing_key := f.Worker.Exchange.OutputRoutingKeys[node_type][routing_key_index]
				message = client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + message
				err := f.Worker.SendMessage(message, routing_key)
				if err != nil {
					return err
				}
				log.Debugf("Sent message to output exchange: %s", message)
			}
		}

	}
	return nil
}

func (f *CommonJoin) RunWorker(ctx context.Context, starting_message string, join_function func(lines []string, movies_by_id map[string]string), storing_function func(line string, movies_by_id map[string]string)) error {

	for {
		msg, inputIndex, err := f.Worker.ReceivedMessages(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("Shutting down message dispatcher gracefully")
				return nil
			}
			log.Errorf("Error receiving messages: %s", err)
			return err
		}
		message_str := string(msg.Body)

		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 3)[0]
		message_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 3)[1]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 3)[2]
		f.EnsureClient(client_id)
		if inputIndex == 0 { // recibiendo movies

			if message_str == worker.MESSAGE_EOF {
				err := f.HandleMovieEOF(client_id, message_id)
				if err != nil {
					return err
				}
				if len(f.Eofs[client_id][client_id]) >= f.Expected_eof {
					f.SendPendings(client_id, join_function)
				}
				msg.Ack(false)
				continue
			}

			line := strings.TrimSpace(message_str)
			storing_function(line, f.Client_movies_by_id[client_id])
			err := common_statefull_worker.StoreElements(f.Client_movies_by_id[client_id], client_id, f.Storage_base_dir)
			if err != nil {
				return err
			}
			msg.Ack(false)

		} else { // recibiendo credits
			if len(f.Eofs[client_id][client_id]) < f.Expected_eof {
				f.HandlePending(client_id, message_id, message_str)
				msg.Ack(false)
				continue
			}

			f.SendPendings(client_id, join_function)

			if message_str == worker.MESSAGE_EOF {
				err := f.HandleJoiningEOF(client_id, message_id)
				if err != nil {
					return err
				}
				msg.Ack(false)
				continue
			}

			err = f.HandleLine(client_id, message_id, message_str, join_function)
			if err != nil {
				return err
			}
			msg.Ack(false)
		}
	}
}
