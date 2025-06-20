import logging
import signal
import socket
import threading
from types import FrameType
from typing import Optional
from .worker import Worker, WorkerConfig
from .client import Client

from common.communication import Socket

FILES_TO_RECEIVE = 3
QUERIES_NUMBER = 5
EOF = "EOF"


class ClientHandlerConfig(WorkerConfig):
    pass


class ClientHandler:
    def __init__(self,
                 port: int,
                 client_handler_config: ClientHandlerConfig,
                 listen_backlog: int,
                 ):
        self._cli_hand_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        self._cli_hand_socket.bind(('', port))
        self._cli_hand_socket.listen(listen_backlog)
        self._running = True

        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

        self.client_handler_config = client_handler_config
        self.worker = Worker(client_handler_config)

        try:
            self.worker.init_receiver()
        except Exception as e:
            logging.error(f"Error initializing worker: {e}")
            return e

        self.eof_per_client: dict[str, int] = {}  # TODO: REMOVE
        self.clients_lock = threading.Lock()
        self.clients: dict[str, Client] = {}

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        self._running = False
        self._cli_hand_socket.close()

    def run(self):
        threading.Thread(target=self.__manage_client_results).start()

        while self._running:
            client_socket = self.__accept_new_connection()
            client_socket = Socket(client_socket)
            threading.Thread(target=self.__handle_client,
                             args=(client_socket,)).start()

    def __handle_client(self, client_socket: Socket):
        client = Client(client_socket, self.client_handler_config)
        with self.clients_lock:
            self.clients[client.worker.client_id] = client
        self.__receive_datasets(client)

    def __receive_datasets(self, client: Client):
        for i in range(FILES_TO_RECEIVE):
            client.receive_first_chunck()
            logging.debug("Receiving file %d of size %d", i,
                          client.batch_processor.read_until)
            logging.info("Received %d bytes out of %d --- file %d",
                         client.batch_processor.bytes_read, client.batch_processor.read_until, i)

            while self._running:
                if client.batch_processor.received_all_data():
                    break
                try:
                    bytes_received, chunck_received = client.read()
                    client.batch_processor.process_batch(
                        bytes_received, chunck_received)
                    client.send_message_to_workers()
                except socket.error as e:
                    logging.error(f'action: receive_datasets | error: {e}')
                    return
            client.send_eof()

            logging.info(
                f'\n--- received file: {i} | file_size: {client.batch_processor.read_until} | received: {client.batch_processor.bytes_read} ---\n')

            client.set_next_processor()

    def __manage_client_results(self):
        for method_frame, _properties, result_encoded in self.worker.received_messages():
            result = result_encoded.decode('utf-8')
            client_id = result.split("|", 2)[0]
            result = result.split("|", 2)[2]
            query_number = method_frame.routing_key.split(".")[0]
            logging.info(
                "Received result for client %s from worker: %s", client_id, result)

            if result == EOF or len(result) == 0:
                self.eof_per_client[client_id] = self.eof_per_client.get(
                    client_id, 0) + 1
                if self.eof_per_client[client_id] >= QUERIES_NUMBER:
                    with self.clients_lock:
                        client = self.clients.pop(client_id)
                    client.send(EOF)
                    client.close()
                    logging.info(
                        f"EOF received for client {client_id} - closing connection")
                continue

            result = f"{client_id}/{query_number}/{result}\n"
            with self.clients_lock:
                client = self.clients.get(client_id)
            client.send(result)

    def __accept_new_connection(self):
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._cli_hand_socket.accept()
        logging.info(
            f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
