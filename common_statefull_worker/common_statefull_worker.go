package common_statefull_worker

import (
	"bufio"
	"context"
	worker "distribuidos-tp1/common/worker/worker"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"strings"

	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type StatefullWorker interface {
	// Updates the internal in-memory state with the given lines for the specified client
	UpdateState(lines []string, client_id string, message_id string) bool
	// Handles EOF message processing for the specified client
	HandleEOF(client_id string, message_id string) error
	// Converts the current state for the specified client to a string representation
	MapToLines(client_id string) string
	// Commits the message if needed
	HandleCommit(client_id string, message amqp091.Delivery) error
	// Init the in memory state for the client if needed
	EnsureClient(client_id string)
}

var log = logging.MustGetLogger("common_group_by")

// send 2 messages to the next node. All statefull workers have exactly one node next
// the first message is the state we want to send, the second one is the EOF
func SendResult(w worker.Worker, client_id string, lines string, message_id string, eof_id string) error {
	var onlyRoutingKeys []string
	for _, keys := range w.Exchange.OutputRoutingKeys {
		onlyRoutingKeys = keys
	}
	if len(onlyRoutingKeys) != 1 {
		log.Errorf("Error: expected exactly one output routing key, got %d", len(onlyRoutingKeys))
		return fmt.Errorf("expected exactly one output routing key, got %d", len(onlyRoutingKeys))
	}
	send_queue_key := onlyRoutingKeys[0]
	if len(lines) != 0 {
		message_to_send := client_id + worker.MESSAGE_SEPARATOR + message_id + worker.MESSAGE_SEPARATOR + lines
		err := w.SendMessage(message_to_send, send_queue_key)
		if err != nil {
			log.Errorf("Error sending message: %s", err.Error())
			return err
		}
	}
	message_to_send := client_id + worker.MESSAGE_SEPARATOR + eof_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
	err := w.SendMessage(message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	log.Debugf("Sent message: %s", message_to_send)
	return nil
}

// Generic way to run a statefullworker.
// first we verify we have the in memory state for this client
// second we update the state for that client
// third we commit the value
// if there is an eof, we handle it different as we don't need to update the state
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
		repeated_message := s.UpdateState(lines, client_id, message_id)
		if repeated_message {
			message.Ack(false)
			continue
		}
		err = s.HandleCommit(client_id, message)
		if err != nil {
			log.Warningf("Error while commiting")
			return err
		}
	}
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

// Verify the last id that is in the inner state of the node with the last message id in the ids append file
// if there is a difference, means we died before appending all the ids, so we have to recover them in the append file of ids
func RestoreStateIfNeeded(last_messages_in_state map[string][]string, last_message_in_ids map[string]string, storage_base_dir string) (bool, error) {
	var clients_ids_to_restore []string
	for client_id, last_movies_id := range last_messages_in_state {
		if len(last_movies_id) == 0 {
			continue
		}
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

// write to a file and sync the results when finished. We don't promise if the flags isn't at append that the file is or not corrupted
func genericWriteToFile(storage_base_dir string, last_message_ids []string, client_id string, dir_name string, flags int) error {
	dir := filepath.Join(storage_base_dir, dir_name)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/%s_ids.txt", dir, client_id)

	f, err := os.OpenFile(filename, flags, 0644)
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
	err = f.Sync()
	if err != nil {
		return err
	}
	return nil
}

// Append the ids given to the file
func appendIds(storage_base_dir string, last_message_ids []string, client_id string) error {
	return genericWriteToFile(storage_base_dir, last_message_ids, client_id, "ids", os.O_APPEND|os.O_CREATE|os.O_WRONLY)
}

// stores the node id that was created. It doesn't matter if there is other data, this will only replace with the new data
// this is needed for group nodes, as they send new messages when they end, we need to create an id for the eof and the message
// that is send. If we created a new one at the moment, there is a possibility that:
// we receive an eof -> we send the message -> we die before .ack the last eof message
// so we resend the data, if we didn't store this when we received the eof again,
// we would send a new message with a new message_id, and the next node wouldn't know it's a repeated message
func StoreMyId(storage_base_dir string, last_message_ids []string, client_id string) error {
	return genericWriteToFile(storage_base_dir, last_message_ids, client_id, "node", os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
}

// This function is used to get data from the states file.
// this pass through all the files from the given directory. The files are in the format
// <client_id><something>. returns a hashmap with the client id as the key and a hashmap of the json that is in the file
// also returns the last_messages of the clients if there are some
func genericGetElements[T any](storage_base_dir string, dir_name string) (map[string]map[string]T, map[string][]string) {
	grouped := make(map[string]map[string]T)
	last_messages := make(map[string][]string)
	dir := fmt.Sprintf("%s/%s", storage_base_dir, dir_name)

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
		var jsonLines []string

		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "LAST_ID"):
				last_messages[client_id] = append(last_messages[client_id], strings.Fields(line)[1])
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

	return grouped, last_messages
}

// get the elements from the pending directory
func GetPending[T any](storage_base_dir string) (map[string]map[string]T, map[string][]string) {
	return genericGetElements[T](storage_base_dir, "pending")
}

// get the elements from the eofs directory
func GetEofs[T any](storage_base_dir string) (map[string]map[string]T, map[string][]string) {
	return genericGetElements[T](storage_base_dir, "eofs")
}

// get the elements from the state directory
func GetElements[T any](storage_base_dir string) (map[string]map[string]T, map[string][]string) {
	return genericGetElements[T](storage_base_dir, "state")
}

// This function is used to get data from the appends file.
// this pass through all the files from the given directory. The files are in the format
// <client_id><something>. returns a hashmap with the client id as the key and all the data found.
func genericGetIds(storage_base_dir string, dir_name string) (map[string][]string, map[string]string) {
	grouped := make(map[string][]string)
	last_messages := make(map[string]string)

	dir := fmt.Sprintf("%s/%s", storage_base_dir, dir_name)
	files, err := os.ReadDir(dir)
	if err != nil {
		return grouped, last_messages
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

// get the ids from the ids directory
func GetIds(storage_base_dir string) (map[string][]string, map[string]string) {
	return genericGetIds(storage_base_dir, "ids")
}

// get the ids from the node directory
func GetMyId(storage_base_dir string) (map[string][]string, map[string]string) {
	return genericGetIds(storage_base_dir, "node")
}

// store data to the state directory and appends the last N messages ids received at the end
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

	err := genericStoreElements(results, client_id, storage_base_dir, after_write_function, "state")
	if err != nil {
		return err
	}
	if len(last_message_ids) == 0 {
		return nil
	}
	return appendIds(storage_base_dir, last_message_ids, client_id)
}

// store data to the eofs directory
func StoreEofsWithId[T any](results map[string]T, client_id, storage_base_dir string) error {
	return genericStoreElements(results, client_id, storage_base_dir, nil, "eofs")
}

// store data to the state directory
func StoreElements[T any](results map[string]T, client_id, storage_base_dir string) error {
	return genericStoreElements(results, client_id, storage_base_dir, nil, "state")
}

// store data to the pending directory
func StorePending[T any](results map[string]T, client_id, storage_base_dir string) error {
	return genericStoreElements(results, client_id, storage_base_dir, nil, "pending")
}

// genericStoreElements is an atomic function to store a file to disk
// First, creates a temporary file to write the data in a json format
// then writes any additional data needed at the end of the json
// if everything was OK, flush to disk the temp file and then use the rename function to <client_id>_commited.txt
// everything that is commited is OK, as the rename function is atomic
func genericStoreElements[T any](results map[string]T, client_id, storage_base_dir string, after_write_function func(f *os.File) error, dir_name string) error {
	dir := filepath.Join(storage_base_dir, dir_name)
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

// genericCleanState deletes all state files for a given client
// storage_base_dir is the base directory where state files are stored
// client_id is the id of the client to delete state files for
// dir_name is the name of the directory where the files are stored
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

// clean state for pending directory, used for joins
func CleanPending(storage_base_dir string, client_id string) {
	genericCleanState(storage_base_dir, client_id, "pending")
}

// clean all state for group and master group by nodes
func CleanGroupNode(storage_base_dir string, client_id string) {
	genericCleanState(storage_base_dir, client_id, "state")
	genericCleanState(storage_base_dir, client_id, "ids")
	genericCleanState(storage_base_dir, client_id, "node")
	genericCleanState(storage_base_dir, client_id, "eofs")
}

// clean all state for join nodes
func CleanJoinNode(storage_base_dir string, client_id string) {
	genericCleanState(storage_base_dir, client_id, "state")
	genericCleanState(storage_base_dir, client_id, "eofs")
	CleanPending(storage_base_dir, client_id)
}

// clean all state for topN nodes
func CleanTopNNode(storage_base_dir string, client_id string) {
	genericCleanState(storage_base_dir, client_id, "state")
	genericCleanState(storage_base_dir, client_id, "node")
}
