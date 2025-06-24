package common_join

import (
	buffer "distribuidos-tp1/common/worker/hasher"
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
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
	common_statefull_worker.CleanState(f.Storage_base_dir, client_id)

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

func (f *CommonJoin) HandleLine(client_id string, message_id string, line string, join_function func(lines []string, movies_by_id map[string]string)) error {
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
