import logging
import signal
import socket
import sys
import threading
from types import FrameType
from typing import Optional
from .worker import Worker, WorkerConfig

from common.communication import Socket
from common.fileProcessor import MoviesProcessor, CreditsProcessor, RatingsProcessor

FILES_TO_RECEIVE = 3


class ClientHandlerConfig(WorkerConfig):
    pass


class ClientHandler:
    def __init__(self,
                 port: int,
                 client_handler_config: ClientHandlerConfig,
                 routing_keys1: list[str],
                 routing_keys2: list[str],
                 routing_keys3: list[str]):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(1)  # TODO: change to .env
        self._client_socket: Optional[Socket] = None

        self._running = True
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

        logging.info(f"routing_keys1: {routing_keys1}")
        logging.info(f"routing_keys2: {routing_keys2}")
        logging.info(f"routing_keys3: {routing_keys3}")

        self.routing_keys1 = routing_keys1
        self.routing_keys2 = routing_keys2
        self.routing_keys3 = routing_keys3

        self.queue_to_send = 0
        self.batch_processor = MoviesProcessor()
        self.worker = Worker(client_handler_config)

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        self._running = False
        if self._client_socket:
            self._client_socket.sock.close()
        self._server_socket.close()
        self.worker.close_worker()

    def run(self):
        try:
            self.worker.init_senders()
            self.worker.init_receiver()
        except Exception as e:
            logging.error(f"Error initializing worker: {e}")
            return e
        result_thread = threading.Thread(target=self.__send_result_to_client)
        result_thread.start()
        while self._running:
            client_socket = self.__accept_new_connection()
            self._client_socket = Socket(client_socket)
            self.__receive_datasets()
        result_thread.join()

    def __receive_datasets(self):
        for i in range(FILES_TO_RECEIVE):
            self.__receive_first_chunck()
            logging.debug("Receiving file %d of size %d", i,
                          self.batch_processor.read_until)
            logging.info("Received %d bytes out of %d --- file %d",
                         self.batch_processor.bytes_read, self.batch_processor.read_until, i)

            j = 0
            while self._running and self._client_socket:
                if self.batch_processor.received_all_data():
                    self.__send_data(self.batch_processor.get_all_data())
                    break
                try:
                    self._send_batch_if_threshold_reached()
                    bytes_received, chunck_received = self._client_socket.read()

                    self.batch_processor.process_batch(
                        bytes_received, chunck_received)

                    # if j > 0:
                    #     sys.stdout.write("\033[F" * 2)
                    percent_bytes_received = (
                        self.batch_processor.bytes_read / self.batch_processor.read_until) * 100
                    percent_bytes_received = f"{percent_bytes_received:05.2f}"
                    # print(
                    #     f"\rReceived {percent_bytes_received}% of File {i}\n"
                    #     f"Lines with errors: {self.batch_processor.errors_per_file}\n"
                    #     f"Lines processed: {self.batch_processor.successful_lines_count}",
                    #     end=""
                    # )
                    j = 1
                except socket.error as e:
                    logging.error(f'action: receive_datasets | error: {e}')
                    return
            self.__send_data("EOF")
            # print()
            logging.info(
                f'\n--- received file: {i} | file_size: {self.batch_processor.read_until} | received: {self.batch_processor.bytes_read} ---\n')

            self.__set_next_processor()

    def _send_batch_if_threshold_reached(self):
        if self.batch_processor.ready_to_send():
            data = self.batch_processor.get_processed_batch()
            self.__send_data(data)
            return ""

    def __send_data(self, data_send: str):
        routing_key = ""
        if type(self.batch_processor) == MoviesProcessor:
            self.queue_to_send = (
                self.queue_to_send + 1) % len(self.routing_keys1)
            exchange = self.worker.output_exchange1
            routing_key = self.routing_keys1[self.queue_to_send]
            if data_send == "EOF":
                for routing_key in self.routing_keys1:
                    self.worker.send_message(data_send, routing_key, exchange)
                logging.info("EOF sent")
                return
        elif type(self.batch_processor) == CreditsProcessor:
            exchange = self.worker.output_exchange2
            if data_send == "EOF":
                for routing_key in self.routing_keys2:
                    self.worker.send_message(data_send, routing_key, exchange)
                return
            else:
                id = data_send.split("|")[0]
                hash = int(id) % len(self.routing_keys2)
                routing_key = self.routing_keys2[hash]

            # for routing_key in self.routing_keys2:
            #     self.worker.send_message(data_send, routing_key, exchange)
        else:  # RatingsProcessor
            if data_send == "EOF":
                for routing_key in self.routing_keys3:
                    self.worker.send_message(data_send, routing_key, exchange)
                return
            self.queue_to_send = (
                self.queue_to_send + 1) % len(self.routing_keys3)
            exchange = self.worker.output_exchange3
            routing_key = self.routing_keys3[self.queue_to_send]

        self.worker.send_message(data_send, routing_key, exchange)

    def __receive_first_chunck(self):
        if self._client_socket is None:
            raise ValueError("Client socket is not connected.")
        bytes_read, chunck_received = self._client_socket.read()

        return self.batch_processor.process_first_batch(bytes_read, chunck_received)

    def __set_next_processor(self):
        if type(self.batch_processor) == MoviesProcessor:
            self.batch_processor = CreditsProcessor()
        elif type(self.batch_processor) == CreditsProcessor:
            self.batch_processor = RatingsProcessor()

    def __send_result_to_client(self):
        logging.info(
            f"Waiting message from Exchange {self.worker.input_exchange.name} - {self.worker.input_exchange.routing_keys}")
        for method_frame, _properties, result_encoded in self.worker.received_messages():
            query_number = method_frame.routing_key.split(".")[0]
            result = result_encoded.decode('utf-8')
            result = f"{query_number}-{result}"

            logging.info("Received result from worker: %s", result)
            self._client_socket.send(result)

    def __accept_new_connection(self):
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(
            f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
