package common_join

import (
	"distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/common_statefull_worker"
	"slices"
	"strings"

	"github.com/op/go-logging"
)

type CommonJoin struct {
	worker.Worker
	Client_movies_by_id map[string]map[string]string
	Received_movies     bool
	Storage_base_dir    string
	Pending             map[string]map[string]string
	eofs                map[string]map[string][]string
	expected_eof        int
}

var log = logging.MustGetLogger("common_join")

func NewCommonJoin(config worker.WorkerConfig, storage_base_dir string) *CommonJoin {
	log.Infof("New join: %+v", config)
	grouped_elements, received_movies, _ := common_statefull_worker.GetElements[string](storage_base_dir)
	pending, _, _ := common_statefull_worker.GetPending[string](storage_base_dir)
	eofs, _, _ := common_statefull_worker.GetEofs[[]string](storage_base_dir)
	worker, err := worker.NewWorker(config, 1)
	if err != nil {
		log.Errorf("Error creating worker: %s", err)
		return nil
	}

	return &CommonJoin{
		Worker:              *worker,
		Client_movies_by_id: grouped_elements,
		Received_movies:     received_movies,
		eofs:                eofs,
		expected_eof:        1,
		Storage_base_dir:    storage_base_dir,
		Pending:             pending,
	}
}

func (f *CommonJoin) HandleJoiningEOF(client_id string, message_id string) error {
	log.Warning("RECIBO EOF DE LOS RATINGS")
	send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
	message_to_send := client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
	err := f.Worker.SendMessage(message_to_send, send_queue_key)
	if err != nil {
		log.Infof("Error sending message: %s", err.Error())
		return err
	}

	delete(f.Client_movies_by_id, client_id)
	common_statefull_worker.CleanState(f.Storage_base_dir, client_id)
	return nil
}

func (f *CommonJoin) HandleMovieEOF(client_id string, message_id string) error {
	log.Warning("RECIBO EOF DE LAS MOVIES")
	if slices.Contains(f.eofs[client_id][client_id], message_id) {
		log.Warning("EOF REPETIDO")
		return nil
	}
	f.eofs[client_id][client_id] = append(f.eofs[client_id][client_id], message_id)
	if len(f.eofs[client_id][client_id]) >= f.expected_eof {
		f.Received_movies = true
		err := common_statefull_worker.StoreEofsWithId(f.eofs[client_id], client_id, f.Storage_base_dir)
		if err != nil {
			return err
		}
		err = common_statefull_worker.StoreElementsWithBoolean(f.Client_movies_by_id[client_id], client_id, f.Storage_base_dir, f.Received_movies)
		if err != nil {
			return err
		}
		delete(f.eofs, client_id)
		return nil
	}
	err := common_statefull_worker.StoreEofsWithId(f.eofs[client_id], client_id, f.Storage_base_dir)
	if err != nil {
		return err
	}
	err = common_statefull_worker.StoreElementsWithBoolean(f.Client_movies_by_id[client_id], client_id, f.Storage_base_dir, f.Received_movies)
	if err != nil {
		return err
	}
	return nil
}

func (f *CommonJoin) EnsureClient(client_id string) {
	if _, ok := f.Client_movies_by_id[client_id]; !ok {
		f.Client_movies_by_id[client_id] = make(map[string]string)
	}
	if _, ok := f.eofs[client_id]; !ok {
		f.eofs[client_id] = make(map[string][]string)
	}
	if _, ok := f.eofs[client_id][client_id]; !ok {
		f.eofs[client_id][client_id] = make([]string, 0)
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

func (f *CommonJoin) HandleLine(client_id string, message_id string, line string, join_function func(lines []string, movies_by_id map[string]string) []string) {
	lines := strings.Split(strings.TrimSpace(line), "\n")
	result := join_function(lines, f.Client_movies_by_id[client_id])
	message_to_send := strings.Join(result, "\n")
	if len(message_to_send) != 0 {
		send_queue_key := f.Worker.Exchange.OutputRoutingKeys[0]
		message_to_send = client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + message_to_send
		err := f.Worker.SendMessage(message_to_send, send_queue_key)
		if err != nil {
			log.Infof("Error sending message: %s", err.Error())
		}
	}
}
