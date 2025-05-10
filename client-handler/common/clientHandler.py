import logging
import signal
import socket
import threading
from types import FrameType
from typing import Optional
from .worker import WorkerConfig
from .client import Client

from common.communication import Socket

FILES_TO_RECEIVE = 3
QUERIES_NUMBER = 5
EOF = "EOF"
AMOUNT_OF_SPAIN_2000 = 3


class ClientHandlerConfig(WorkerConfig):
    pass


class ClientHandler:
    def __init__(self,
                 port: int,
                 client_handler_config: ClientHandlerConfig,
                 ):
        self._cli_hand_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        self._cli_hand_socket.bind(('', port))
        self._cli_hand_socket.listen(5)  # TODO: change to .env

        self._running = True
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

        self.client_handler_config = client_handler_config
        self.eof_per_client: dict[str, int] = {}

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        self._running = False
        self._cli_hand_socket.close()

    def run(self):
        while self._running:
            client_socket = self.__accept_new_connection()
            client_socket = Socket(client_socket)
            client_thread = threading.Thread(
                target=self.__handle_client, args=(client_socket,))
            client_thread.start()

    def __handle_client(self, client_socket):

        client = Client(client_socket, self.client_handler_config)

        result_thread = threading.Thread(
            target=self.__send_result_to_client, args=(client, ))
        sender_thread = threading.Thread(
            target=self.__receive_datasets, args=(client, ))
        result_thread.start()
        sender_thread.start()
        result_thread.join()
        sender_thread.join()

    def __receive_datasets(self, client):
        for i in range(FILES_TO_RECEIVE):
            client.receive_first_chunck()
            logging.debug("Receiving file %d of size %d", i,
                          client.batch_processor.read_until)
            logging.info("Received %d bytes out of %d --- file %d",
                         client.batch_processor.bytes_read, client.batch_processor.read_until, i)

            while self._running:
                if client.batch_processor.received_all_data():
                    # client.send_message()
                    break
                try:
                    # client.send_batch_if_threshold_reached()
                    bytes_received, chunck_received = client.read()

                    client.batch_processor.process_batch(
                        bytes_received, chunck_received)

                    client.send_message_to_workers()

                    # percent_bytes_received = (
                    #     client.batch_processor.bytes_read / client.batch_processor.read_until) * 100
                    # percent_bytes_received = f"{percent_bytes_received:05.2f}"

                except socket.error as e:
                    logging.error(f'action: receive_datasets | error: {e}')
                    return
            client.send_eof()

            logging.info(
                f'\n--- received file: {i} | file_size: {client.batch_processor.read_until} | received: {client.batch_processor.bytes_read} ---\n')

            client.set_next_processor()

    def __send_result_to_client(self, client):
        logging.info(
            f"Waiting message from Exchange {client.worker.input_exchange.name} - {client.worker.input_exchange.routing_keys}")
        for method_frame, _properties, result_encoded in client.worker.received_messages():
            result = result_encoded.decode('utf-8')
            client_id = result.split("|", 1)[0]
            result = result.split("|", 1)[1]
            query_number = method_frame.routing_key.split(".")[0]
            logging.info(
                "Received result for client %s from worker: %s", client_id, result)

            if result == EOF or len(result) == 0:
                # self.eof_per_client[client_id] = self.eof_per_client.get(
                #     client_id, 0) + 1
                # if self.eof_per_client[client_id] >= QUERIES_NUMBER + AMOUNT_OF_SPAIN_2000 - 1:
                #     client.send_eof()
                #     logging.info(
                #         f"EOF received for client {client_id} - closing connection")
                #     client.client_socket.sock.close()
                #     self.eof_per_client.pop(client_id)
                #     return
                continue

            result = f"{client_id}/{query_number}/{result}\n"
            client.send(result)

    def __accept_new_connection(self):
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._cli_hand_socket.accept()
        logging.info(
            f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
