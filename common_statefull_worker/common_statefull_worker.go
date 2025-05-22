package common_statefull_worker

import (
	"bufio"
	worker "distribuidos-tp1/common/worker/worker"
	"encoding/json"
	"fmt"
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
	send_queue_key := w.OutputExchange.RoutingKeys[0] // POR QUE VA A ENVIAR A UN UNICO NODO MAESTRO
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
	return nil
}

func Init(w *worker.Worker, starting_message string) (<-chan amqp091.Delivery, error) {
	log.Info(starting_message)

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

	messages_before_commit := 0
	for message := range msgs {
		message_str := string(message.Body)
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
		messages_before_commit += 1
		lines := strings.Split(strings.TrimSpace(message_str), "\n")
		s.UpdateState(lines, client_id)
		if s.ShouldCommit(messages_before_commit, client_id) {
			messages_before_commit = 0
		}
		message.Ack(false)
	}

	return nil
}

// hay 2 tipos de errores.
// error tipo 1) -> "insalvables", en este caso vamos a poner que este nodo va a dejar de brindar servicio
// a ese client id ya que tiene su estado corrupto, por lo que cuando lea un mensaje de un id de cliente que tiene como "muerto"
// va a reencolarlo asi lo agarra otra persona.
// Si todos los nodos con node_name distintos murieron una vez para ese cliente,
// No queda de otra mas que enviar con solo ese ultimo nodo que de alguna forma sepa decir "ok, soy el chavon que murio ultimo para este cliente
// no puedo dejar de ignorar los mensjaes", por lo tanto va a enviar con su estado corrupto y te va a enviar lo que pueda

// error tipo 2) -> salvable. se escribio todo bien el log, pero murio justo una linea antes de enviar el ACK
// aca, ver que carajos hacer, sexont

// casos posibles:
// si el archivo no existe y lo creo, entonces es la primera vez que voy a comitear, esta bien
// si el archivo ya existe y esta en blanco -> Hubo error de tipo 1)
// si el archivo ya existe y esta con datos corrompidos -> hubo error de tipo 1)
// si el archivo ya existe y los horarios estan incorrectos -> hubo error de tipo 1)
// si el archivo ya existe y esta todo bien, pudo o no haber pasado error de tipo 2), no lo sabemos...
// la solucion que se me ocurre es quiza mandar los timestamps en los mensajes? quiza podemos rescatar algo de eso

// node_name deberia ser algo como por ej group-by-country-sum-1 (los mismos nombres que usamos para los containers de docker seria lo ideal creo yo)
func StoreElements[T any](results map[string]T, client_id, node_name string) {

	dir := filepath.Join("logs", node_name)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic("Che, si no puedo crear el directorio medio que cagamos fuego")
	}
	filename := fmt.Sprintf("%s/%s.txt", dir, client_id)
	if info, err := os.Stat(filename); err == nil {
		if info.Size() == 0 {
			panic("Ya existia el archivo, es uno de los errores :) (abrio el archivo, empezo a escribir pero dejo todo en blanco)")
		}
	}

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic("Si no pudimos crear el archivo, medio que cagamos fuego tambien")
		// digamos que todos los panic van a ser error de tipo 1, hay que cambiarlo a que haga el handleo especifico para decir
		// "che, a este cliente le empiezo a rebotar los paquetes"
	}
	defer f.Close()

	initTime := time.Now().UTC().Format(time.RFC3339)
	fmt.Fprintf(f, "INIT %s\n", initTime)

	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		panic(err)
	}
	f.Write(data)
	f.Write([]byte("\n"))

	endTime := time.Now().UTC().Format(time.RFC3339)
	fmt.Fprintf(f, "END %s\n", endTime)
}

func GetElements[T any](node_name string) map[string]map[string]T {
	grouped := make(map[string]map[string]T)

	dir := fmt.Sprintf("logs/%s", node_name)
	files, err := os.ReadDir(dir)
	if err != nil {
		return grouped // si no existia el directorio, te devuelvo las cosas como vacios
	}

	for _, entry := range files {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".txt") {
			continue
		}
		clientID := strings.TrimSuffix(entry.Name(), ".txt")

		filePath := fmt.Sprintf("%s/%s", dir, entry.Name())
		f, err := os.Open(filePath)
		if err != nil {
			panic("NO PUDE ABRIR EL ARCHIVO, ERROR TIPO 1")
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
			panic("MAL FORMATEADO, ERROR TIPO 1")
		}
		endTime, err := time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			panic("MAL FORMATEADO, ERROR TIPO 1")
		}
		if endTime.Before(initTime) {
			panic("END ES ANTERIOR A INIT, POR LO QUE ESCRIBIO HASTA LA MITAD. ERROR TIPO 1") // este es uno de los errores
		}

		var inner map[string]T
		err = json.Unmarshal([]byte(strings.Join(jsonLines, "\n")), &inner)
		if err != nil {
			panic("LOS DATOS DEL MAP ESTAN MAL FORMATEADOS, ERROR TIPO 1")
		}
		grouped[clientID] = inner
	}

	return grouped
}
