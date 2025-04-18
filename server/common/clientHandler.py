import logging
import signal
import socket
from types import FrameType
from typing import Optional

from common.communication import read_from_socket
from common.fileProcessor import MoviesProcessor, CreditsProcessor, RatingsProcessor

FILES_TO_RECEIVE = 3
MAX_BATCH_SIZE = 8000 - 4  # 4 bytes for the file size


class ClientHandler:
    def __init__(self, port: int):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(1)  # TODO: change to .env
        self._client_socket: Optional[socket.socket] = None

        self._running = True
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

        self.batch_processor = RatingsProcessor()

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        self._running = False
        if self._client_socket:
            self._client_socket.close()
        self._server_socket.close()

    def run(self):
        self._client_socket = self.__accept_new_connection()
        self.__receive_datasets()

    def __receive_datasets(self):
        for i in range(FILES_TO_RECEIVE):
            received_data_size, file_size = self.__receive_first_chunck(
                self.batch_processor)
            logging.debug("Receiving file %d of size %d", i, file_size)
            logging.info("Received %d bytes out of %d --- file %d",
                         received_data_size, file_size, i)

            while self._running and self._client_socket and received_data_size < file_size:
                try:
                    bytes_received, chunck_received = read_from_socket(
                        self._client_socket)

                    received_data_size += bytes_received
                    logging.info("Received %d bytes out of %d --- file %d",
                                 received_data_size, file_size, i)

                    self.batch_processor.process_batch(chunck_received)
                    if received_data_size >= file_size:
                        self.__send_data(self.batch_processor.get_all_data())
                        break

                    self._send_batch_if_threshold_reached()
                except socket.error as e:
                    logging.error(f'action: receive_datasets | error: {e}')
                    return

            logging.info(
                f'\n--- received file: {i} | file_size: {file_size} | received: {received_data_size} ---\n')

            self.__set_next_processor()

    def _send_batch_if_threshold_reached(self):
        if self.batch_processor.ready_to_send():
            # TODO: Send the chunk to RabbitMQ
            # We save the processed chunk to a file for now for testing purposes
            data = self.batch_processor.get_processed_batch()
            self.__send_data(data)
            return ""

    def __send_data(self, data_send: str):
        with open(f'file_{type(self.batch_processor).__name__}.csv', 'a') as f:
            f.write(data_send)

    def __receive_first_chunck(self, processor_chunck: MoviesProcessor | CreditsProcessor | RatingsProcessor) -> tuple[int, int]:
        if self._client_socket is None:
            raise ValueError("Client socket is not connected.")
        bytes_read, chunck_received = read_from_socket(self._client_socket)

        return bytes_read, processor_chunck.process_first_batch(chunck_received)

    def __set_next_processor(self):
        if type(self.batch_processor) == CreditsProcessor:
            self.batch_processor = CreditsProcessor()
        elif type(self.batch_processor) == CreditsProcessor:
            self.batch_processor = RatingsProcessor()

    def __accept_new_connection(self):
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(
            f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
