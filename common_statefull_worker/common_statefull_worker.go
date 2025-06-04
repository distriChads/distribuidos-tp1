package common_statefull_worker

import (
	"bufio"
	worker "distribuidos-tp1/common/worker/worker"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"strings"

	"github.com/op/go-logging"
	"github.com/rabbitmq/amqp091-go"
)

type StatefullWorker interface {
	UpdateState(lines []string, client_id string)
	HandleEOF(client_id string) error
	MapToLines(client_id string) string
	ShouldCommit(messages_before_commit int, client_id string) bool
	NewClient(client_id string)
}

var log = logging.MustGetLogger("common_group_by")

func SendResult(w worker.Worker, s StatefullWorker, client_id string) error {
	send_queue_key := w.Exchange.OutputRoutingKeys[0] // POR QUE VA A ENVIAR A UN UNICO NODO MAESTRO
	message_to_send := client_id + worker.MESSAGE_SEPARATOR + s.MapToLines(client_id)
	err := worker.SendMessage(w, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	message_to_send = client_id + worker.MESSAGE_SEPARATOR + worker.MESSAGE_EOF
	err = worker.SendMessage(w, message_to_send, send_queue_key)
	if err != nil {
		log.Errorf("Error sending message: %s", err.Error())
		return err
	}
	log.Debugf("Sent message: %s", message_to_send)
	return nil
}

func Init(w *worker.Worker, starting_message string) (<-chan amqp091.Delivery, error) {
	log.Info(starting_message)
	log.Debug("in debug")

	err := worker.InitSender(w)
	if err != nil {
		return nil, err
	}

	err = worker.InitReceiver(w)

	if err != nil {
		return nil, err
	}

	msgs, err := worker.ReceivedMessages(*w)
	if err != nil {
		return nil, err
	}

	return msgs, nil
}

func RunWorker(s StatefullWorker, msgs <-chan amqp091.Delivery) error {

	messages_since_last_commit := 0
	for message := range msgs {
		message_str := string(message.Body)
		log.Debugf("Received message: %s", message_str)
		client_id := strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[0]
		message_str = strings.SplitN(message_str, worker.MESSAGE_SEPARATOR, 2)[1]
		s.NewClient(client_id)

		if message_str == worker.MESSAGE_EOF {
			err := s.HandleEOF(client_id)
			if err != nil {
				return err
			}
			message.Ack(false)
			continue
		}
		messages_since_last_commit += 1
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		s.UpdateState(lines, client_id)
		if s.ShouldCommit(messages_since_last_commit, client_id) {
			messages_since_last_commit = 0
		}
		message.Ack(false)
	}

	return nil
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

// node_name deberia ser algo como por ej group-by-country-sum-1 (los mismos nombres que usamos para los containers de docker seria lo ideal creo yo)
func StoreElements[T any](results map[string]T, client_id, node_name string, replicas int) {
	dir := filepath.Join("logs", node_name)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic("Che, si no puedo crear el directorio medio que cagamos fuego")
	}
	filename := fmt.Sprintf("%s/%s_0.txt", dir, client_id)

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic("Si no pudimos crear el archivo, medio que cagamos fuego tambien")
	}
	defer f.Close()

	initTime := time.Now().UTC().Format(time.RFC3339)
	fmt.Fprintf(f, "INIT %s\n", initTime) // los Fprintf tienen short writes?

	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		panic(err)
	}
	f.Write(data)
	f.Write([]byte("\n"))

	endTime := time.Now().UTC().Format(time.RFC3339)
	fmt.Fprintf(f, "END %s\n", endTime)

	for i := 1; i <= replicas; i++ {
		replicaFilename := fmt.Sprintf("%s/%s_%d.txt", dir, client_id, i)
		err := copyFile(filename, replicaFilename)
		if err != nil {
			log.Infof("Error creando la replica, no nos molesta realmente %d\n", i)
		}
	}
}

func copyFile(filename_to_copy string, filename_destination string) error {
	original, err := os.Open(filename_to_copy)
	if err != nil {
		return err
	}
	defer original.Close()

	destination, err := os.Create(filename_destination)
	if err != nil {
		return err
	}
	defer destination.Close()

	// Copy tiene short writes?
	_, err = io.Copy(destination, original)
	return err
}

func GetElements[T any](node_name string, number_of_logs_to_search int) (map[string]map[string]T, []string) {
	grouped := make(map[string]map[string]T)

	dir := fmt.Sprintf("logs/%s", node_name)
	visited_clients := make(map[string]bool)
	client_counter := make(map[string]int)
	files, err := os.ReadDir(dir)
	if err != nil {
		return grouped, nil // si no existia el directorio, te devuelvo las cosas como vacios
	}

	for _, entry := range files {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".txt") {
			continue
		}
		clientID := strings.Split(entry.Name(), "_")[0]
		if visited_clients[clientID] { // si ya te marque que tengo un estado OK, empeza a ignorar el resto de archivos de este cliente
			continue
		}
		client_counter[clientID] = client_counter[clientID] + 1

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

		scanner := bufio.NewScanner(f)
		var jsonLines []string
		var initTimeStr, endTimeStr string

		for scanner.Scan() {
			line := scanner.Text()

			switch {
			case strings.HasPrefix(line, "INIT"):
				initTimeStr = strings.Fields(line)[1]

			case strings.HasPrefix(line, "END"):
				endTimeStr = strings.Fields(line)[1]

			default:
				jsonLines = append(jsonLines, line)
			}
		}

		f.Close()

		initTime, err := time.Parse(time.RFC3339, initTimeStr)
		if err != nil {
			continue
		}
		endTime, err := time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			continue
		}
		if endTime.Before(initTime) {
			continue
		}

		var inner map[string]T
		err = json.Unmarshal([]byte(strings.Join(jsonLines, "\n")), &inner)
		if err != nil {
			continue
		}
		grouped[clientID] = inner
		visited_clients[clientID] = true
	}
	var clients_with_wrong_state []string
	for key, value := range client_counter {

		if !visited_clients[key] && value == number_of_logs_to_search+1 {
			clients_with_wrong_state = append(clients_with_wrong_state, key)
		}
	}

	if len(clients_with_wrong_state) != 0 {
		return grouped, clients_with_wrong_state // aca te paso esto, seguramente lo tengamos que guardar en algun lado en el nodo
		// asi sabe a que clientes le termina de mandar el EOF-FAIL :)
	}

	return grouped, nil
}
