package common_statefull_worker

import (
	"bufio"
	"context"
	worker "distribuidos-tp1/common/worker/worker"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"strings"

	"github.com/google/uuid"
	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type StatefullWorker interface {
	// Updates the internal in-memory state with the given lines for the specified client
	UpdateState(lines []string, client_id string, message_id string)
	// Handles EOF message processing for the specified client
	HandleEOF(client_id string, message_id string) error
	// Converts the current state for the specified client to a string representation
	MapToLines(client_id string) string
	HandleCommit(client_id string, message amqp091.Delivery) error
	EnsureClient(client_id string)
}

var log = logging.MustGetLogger("common_group_by")

func SendResult(w worker.Worker, s StatefullWorker, client_id string) error {
	send_queue_key := w.Exchange.OutputRoutingKeys[0]
	message_id, err := uuid.NewRandom()
	if err != nil {
		log.Errorf("Error generating uuid: %s", err.Error())
		return err
	}
	message_to_send := client_id + worker.MESSAGE_SEPARATOR + message_id.String() + worker.MESSAGE_SEPARATOR + s.MapToLines(client_id)
	err = w.SendMessage(message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	message_id, err = uuid.NewRandom()
	if err != nil {
		log.Errorf("Error generating uuid: %s", err.Error())
		return err
	}
	message_to_send = client_id + worker.MESSAGE_SEPARATOR + message_id.String() + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
	err = w.SendMessage(message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	log.Debugf("Sent message: %s", message_to_send)
	return nil
}

func RunWorker(s StatefullWorker, ctx context.Context, w worker.Worker, starting_message string) error {
	log.Info(starting_message)

	for {
		message, _, err := w.ReceivedMessages(ctx)
		if err != nil {
			log.Errorf("Fatal error in run worker: %v", err)
			return err
		}
		message_str := string(message.Body)
		log.Debugf("Received message: %s", message_str)
		if len(message_str) == 0 {
			message.Ack(false)
			continue
		}
		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 3)[0]
		message_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 3)[1]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 3)[2]
		s.EnsureClient(client_id)

		if message_str == worker.MESSAGE_EOF {
			err := s.HandleEOF(client_id, message_id)
			if err != nil {
				return err
			}
			message.Ack(false)
			continue
		}

		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		s.UpdateState(lines, client_id, message_id)
		err = s.HandleCommit(client_id, message)
		if err != nil {
			log.Warningf("Error while commiting")
			return err
		}
	}
}

// Verify the last id that is in the inner state of the node with the last message id in the ids append file
// if there is a difference, means we died before appending all the ids, so we have to recover them in the append file of ids
func RestoreStateIfNeeded(last_messages_in_state map[string][]string, last_message_in_ids map[string]string, storage_base_dir string) (bool, error) {
	var clients_ids_to_restore []string
	for client_id, last_movies_id := range last_messages_in_state {
		if last_message_in_ids[client_id] != last_movies_id[len(last_movies_id)-1] {
			clients_ids_to_restore = append(clients_ids_to_restore, client_id)
		}
	}

	for _, client_id := range clients_ids_to_restore {
		err := appendIds(storage_base_dir, last_messages_in_state[client_id], client_id)
		if err != nil {
			return false, err
		}
	}

	return len(clients_ids_to_restore) != 0, nil
}

// hay 2 tipos de errores.
// error tipo 1) -> "insalvables", la idea es la siguiente
// voy a intentar leer de un archivo, si este esta corrupto voy al siguiente. Si este esta corrupto al siguiente y asi
// hasta pasar por los N archivos que se eligieron como posible backup.
// si NINGUNO de los archivos tenia estado, okay, se va a aceptar que hubo un error fatal, se va a colocar el estado en blanco
// para este nodo y se va a seguir brindando servicio normalmente, pero se va a enviar un EOF-FAIL en vez de un EOF normal, asi lo sabe
// el cliente que "che, te di un resultado pero es cualquier banana, vos fijate"

// error tipo 2) -> salvable. se escribio todo bien el log, pero murio justo una linea antes de enviar el ACK
// interesados -> client handler, group by (ver como manejar estos casos...)
// interesado facil de arreglar ya que no mantiene un estado de agregacion -> topN (solo hacer un if si esta repetido cuando lo tenga guardado, ignorar)
// NO interesados -> filtros, ML, join (a cualquiera de estos si les llega un repetido, van a pisar estado o simplemente reenviar un proximo, no hay drama)

// casos posibles:
// si el archivo no existe y lo creo, entonces es la primera vez que voy a comitear, esta bien
// si el archivo ya existe y esta en blanco -> Hubo error de tipo 1)
// si el archivo ya existe y esta con datos corrompidos -> hubo error de tipo 1)
// si el archivo ya existe y los horarios estan incorrectos -> hubo error de tipo 1)
// si el archivo ya existe y esta todo bien, pudo o no haber pasado error de tipo 2), no lo sabemos...
// la solucion que se me ocurre es quiza mandar los timestamps en los mensajes? quiza podemos rescatar algo de eso

func appendIds(storage_base_dir string, last_message_ids []string, client_id string) error {
	dir := filepath.Join(storage_base_dir, "ids")
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/%s_ids.txt", dir, client_id)

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, line := range last_message_ids {
		line := fmt.Sprintf("%s\n", line)
		if err := doWrite([]byte(line), f); err != nil {
			return err
		}
	}
	return err
}

// StoreElements stores the state for a given client in the given storage base directory
// results is the state to store
// client_id is the id of the client to store the state for
// storage_base_dir is the base directory where state files are stored
// replicas is the number of replicas to create
func StoreElementsWithMessageIds[T any](
	results map[string]T,
	client_id, storage_base_dir string,
	last_message_ids []string,
) error {
	after_write_function := func(f *os.File) error {
		for _, line := range last_message_ids {
			line := fmt.Sprintf("LAST_ID %s\n", line)
			err := doWrite([]byte(line), f)
			if err != nil {
				return err
			}
		}
		return nil
	}

	err := genericStoreElements(results, client_id, storage_base_dir, after_write_function)
	if err != nil {
		return err
	}

	return appendIds(storage_base_dir, last_message_ids, client_id)
}

// Store the state and a boolean at the end
func StoreElementsWithBoolean[T any](
	results map[string]T,
	client_id, storage_base_dir string,
	boolean bool,
) error {
	after_write_function := func(f *os.File) error {
		line := fmt.Sprintf("BOOLEAN %t\n", boolean)
		return doWrite([]byte(line), f)
	}

	return genericStoreElements(results, client_id, storage_base_dir, after_write_function)

}

func GetIds(storage_base_dir string) (map[string][]string, map[string]string) {
	grouped := make(map[string][]string)
	last_messages := make(map[string]string)

	dir := fmt.Sprintf("%s/ids", storage_base_dir)
	files, err := os.ReadDir(dir)
	if err != nil {
		return grouped, last_messages // si no existia el directorio, te devuelvo las cosas como vacios
	}

	for _, entry := range files {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".txt") {
			continue
		}
		client_id := strings.Split(entry.Name(), "_")[0]

		filePath := fmt.Sprintf("%s/%s", dir, entry.Name())
		if info, err := os.Stat(filePath); err == nil {
			if info.Size() == 0 {
				continue
			}
		}
		f, err := os.Open(filePath)
		if err != nil {
			continue
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		var ids []string

		for scanner.Scan() {
			line := scanner.Text()
			ids = append(ids, line)
			last_messages[client_id] = line
		}

		grouped[client_id] = ids

	}

	return grouped, last_messages
}

func StoreElements[T any](results map[string]T, client_id, storage_base_dir string) error {
	return genericStoreElements(results, client_id, storage_base_dir, nil)
}

// No short-write function
func doWrite(data []byte, file *os.File) error {
	data_written_so_far := 0
	for data_written_so_far < len(data) {
		data_written, err := file.Write(data[data_written_so_far:])
		if err != nil {
			return err
		}
		data_written_so_far += data_written
	}
	return nil
}

func genericStoreElements[T any](results map[string]T, client_id, storage_base_dir string, after_write_function func(f *os.File) error) error {
	dir := filepath.Join(storage_base_dir, "state")
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		log.Criticalf("Error creating directory: %s", err.Error())
		return err
	}

	commited_filename := filepath.Join(dir, fmt.Sprintf("%s_commited.txt", client_id))

	tmpFile, err := os.CreateTemp(dir, client_id+"_*.tmp")
	if err != nil {
		return err
	}

	tmpFilePath := tmpFile.Name()
	cleanup := func() {
		tmpFile.Close()
		os.Remove(tmpFilePath)
	}

	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		cleanup()
		return err
	}
	if err := doWrite(data, tmpFile); err != nil {
		cleanup()
		return err
	}
	if err := doWrite([]byte("\n"), tmpFile); err != nil {
		cleanup()
		return err
	}

	if after_write_function != nil {
		after_write_function(tmpFile)
	}

	if err := tmpFile.Sync(); err != nil {
		cleanup()
		return err
	}

	if err := os.Rename(tmpFile.Name(), commited_filename); err != nil {
		cleanup()
		return err
	}
	// como lo renombre, ya no existe por lo que no hago el remove :) medio feo por que no puedo usar defer
	tmpFile.Close()
	return nil
}

func GetElements[T any](storage_base_dir string) (map[string]map[string]T, bool, map[string][]string) {
	grouped := make(map[string]map[string]T)
	last_messages := make(map[string][]string)
	boolean := false
	dir := fmt.Sprintf("%s/state", storage_base_dir)

	files, err := os.ReadDir(dir)
	if err != nil {
		return grouped, boolean, last_messages // si no existia el directorio, te devuelvo las cosas como vacios
	}

	for _, entry := range files {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".txt") {
			continue
		}
		client_id := strings.Split(entry.Name(), "_")[0]

		filePath := fmt.Sprintf("%s/%s", dir, entry.Name())
		if info, err := os.Stat(filePath); err == nil {
			if info.Size() == 0 {
				continue
			}
		}
		f, err := os.Open(filePath)
		if err != nil {
			continue
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		var jsonLines []string

		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "LAST_ID"):
				last_messages[client_id] = append(last_messages[client_id], strings.Fields(line)[1])
			case strings.HasPrefix(line, "BOOLEAN"):
				boolean, err = strconv.ParseBool(strings.Fields(line)[1])
				if err != nil {
					continue
				}
			default:
				jsonLines = append(jsonLines, line)
			}
		}

		var inner map[string]T
		err = json.Unmarshal([]byte(strings.Join(jsonLines, "\n")), &inner)
		if err != nil {
			continue
		}
		grouped[client_id] = inner

	}

	return grouped, boolean, last_messages
}

func genericCleanState(storage_base_dir string, client_id string, dir_name string) {
	dir := fmt.Sprintf("%s/%s", storage_base_dir, dir_name)
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Warningf("Error reading state directory for deletion: %s", err.Error())
	}
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".txt") || !strings.HasPrefix(file.Name(), client_id) {
			continue
		}
		err := os.Remove(fmt.Sprintf("%s/%s", dir, file.Name()))
		if err != nil {
			log.Warningf("Error deleting state file %s: %s", file.Name(), err.Error())
		}
	}
}

// CleanState deletes all state files for a given client
// storage_base_dir is the base directory where state files are stored
// client_id is the id of the client to delete state files for
func CleanState(storage_base_dir string, client_id string) {
	genericCleanState(storage_base_dir, client_id, "state")
	genericCleanState(storage_base_dir, client_id, "ids")
}
