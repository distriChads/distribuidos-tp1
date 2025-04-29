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
        self._server_socket.listen(5)  # TODO: change to .env

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
        self.client_handler_config = client_handler_config

       

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        self._running = False
        self._server_socket.close()

    def run(self):
        while self._running:
            client_socket = self.__accept_new_connection()
            client_socket = Socket(client_socket)
            client_thread = threading.Thread(target=self.__handle_client, args=(client_socket,))
            client_thread.start()


    def __handle_client(self, client_socket):
        # TODO OBJETO "CLIENT" QUE MANTENGA EL CLIENT_SOCKET, BATCH_PROCESSOR Y EL WORKER
        worker = Worker(self.client_handler_config)
        try:
            worker.init_senders()
            worker.init_receiver()
        except Exception as e:
            logging.error(f"Error initializing worker: {e}")
            return e
        batch_processor = MoviesProcessor()
        result_thread = threading.Thread(target=self.__send_result_to_client, args=(client_socket, worker, ))
        sender_thread = threading.Thread(target=self.__receive_datasets, args=(client_socket, batch_processor, worker, ))
        result_thread.start()
        sender_thread.start()
        result_thread.join()
        sender_thread.join()

    def __receive_datasets(self, client_socket, batch_processor, worker):
        for i in range(FILES_TO_RECEIVE):
            queue_to_send = 0
            self.__receive_first_chunck(client_socket, batch_processor)
            logging.debug("Receiving file %d of size %d", i,
                            batch_processor.read_until)
            logging.info("Received %d bytes out of %d --- file %d",
                         batch_processor.bytes_read, batch_processor.read_until, i)

            j = 0
            while self._running and client_socket:
                if batch_processor.received_all_data():
                    queue_to_send = self.__send_data(batch_processor.get_all_data(), batch_processor,queue_to_send, worker)
                    break
                try:
                    queue_to_send = self._send_batch_if_threshold_reached(batch_processor, queue_to_send, worker)
                    bytes_received, chunck_received = client_socket.read()

                    batch_processor.process_batch(
                        bytes_received, chunck_received)


                    percent_bytes_received = (
                        batch_processor.bytes_read / batch_processor.read_until) * 100
                    percent_bytes_received = f"{percent_bytes_received:05.2f}"

                except socket.error as e:
                    logging.error(f'action: receive_datasets | error: {e}')
                    return
            queue_to_send = self.__send_data("EOF", batch_processor, queue_to_send, worker)
 
            logging.info(
                f'\n--- received file: {i} | file_size: {batch_processor.read_until} | received: {batch_processor.bytes_read} ---\n')

            batch_processor = self.__set_next_processor(batch_processor)

    def _send_batch_if_threshold_reached(self, batch_processor,queue_to_send, worker):
        if batch_processor.ready_to_send():
            data = batch_processor.get_processed_batch()
            queue_to_send = self.__send_data(data, batch_processor, queue_to_send, worker)
            return queue_to_send
        return queue_to_send

    def __send_data(self, data_send: str, batch_processor, queue_to_send, worker):
        routing_key = ""
        if type(batch_processor) == MoviesProcessor:
            queue_to_send = (
                queue_to_send + 1) % len(self.routing_keys1)
            exchange = worker.output_exchange1
            routing_key = self.routing_keys1[queue_to_send]
            if data_send == "EOF":
                for routing_key in self.routing_keys1:
                    worker.send_message(data_send, routing_key, exchange)
                logging.info("EOF sent")
                return queue_to_send
        elif type(batch_processor) == CreditsProcessor:
            exchange = worker.output_exchange2
            if data_send == "EOF":
                for routing_key in self.routing_keys2:
                    worker.send_message(data_send, routing_key, exchange)
                return queue_to_send
            else:
                id = data_send.split("|")[0]
                hash = int(id) % len(self.routing_keys2)
                routing_key = self.routing_keys2[hash]

            # for routing_key in self.routing_keys2:
            #     worker.send_message(data_send, routing_key, exchange)
        else:  # RatingsProcessor
            exchange = worker.output_exchange3
            if data_send == "EOF":
                for routing_key in self.routing_keys3:
                    worker.send_message(data_send, routing_key, exchange)
                return queue_to_send
            queue_to_send = (
                queue_to_send + 1) % len(self.routing_keys3)
            routing_key = self.routing_keys3[queue_to_send]

        worker.send_message(data_send, routing_key, exchange)
        return queue_to_send

    def __receive_first_chunck(self, client_socket, batch_processor):
        if client_socket is None:
            raise ValueError("Client socket is not connected.")
        bytes_read, chunck_received = client_socket.read()

        return batch_processor.process_first_batch(bytes_read, chunck_received)

    def __set_next_processor(self, batch_processor):
        if type(batch_processor) == MoviesProcessor:
            return CreditsProcessor()
        elif type(batch_processor) == CreditsProcessor:
            return RatingsProcessor()

    def __send_result_to_client(self, client_socket, worker):
        logging.info(
            f"Waiting message from Exchange {worker.input_exchange.name} - {worker.input_exchange.routing_keys}")
        for method_frame, _properties, result_encoded in worker.received_messages():
            client_id = method_frame.routing_key.split(".")[0]  # los routing key formato = *.results.*
            query_number = method_frame.routing_key.split(".")[1]
            result = result_encoded.decode('utf-8')
            if result == "EOF":
                continue
            result = f"{client_id}/{query_number}/{result}\n"

            logging.info("Received result from worker: %s", result)
            client_socket.send(result)

    def __accept_new_connection(self):
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(
            f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
