import logging
import os
import signal
import socket
from types import FrameType
from typing import Optional

from communication import write_to_socket


MOVIES_PATH = "../datasets/movies_metadata.csv"
CREDITS_PATH = "../datasets/credits.csv"
RATINGS_PATH = "../datasets/ratings.csv"

BATCH_SIZE = 8000 - 4  # 4 bytes for the length of the message


class Client:
    def __init__(self, server_port: int):
        self.server_port = server_port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.running = True
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        self.running = False
        self.client_socket.close()
        logging.info("Client socket closed")

    def __connect(self):
        self.client_socket.connect(('localhost', self.server_port))
        logging.info(f"Connected to server")

    def run(self):
        self.__connect()

        try:
            self.__send_file_in_chunks(MOVIES_PATH)
            self.__send_credits_in_chunks(CREDITS_PATH)
            self.__send_file_in_chunks(RATINGS_PATH)

        except Exception as e:
            logging.error(f"Error: {e}")

        self.__graceful_shutdown_handler()

    def __send_file_in_chunks(self, file_path: str):
        """
        Send a file in chunks to the server
        :param file_path: path to the file
        """
        file_size = os.path.getsize(file_path)
        total_msg_bytes = len(str(file_size)) + 1 + file_size
        file_transfer_header = str(total_msg_bytes).encode('utf-8')
        file_name = file_path.split("/")[-1]
        logging.info(
            f"Started sending {total_msg_bytes}B to server for {file_name}")

        buffer = b""
        with open(file_path, 'rb') as file:
            # 4 bytes for length + 1 byte for delimiter
            chunk = file.read(BATCH_SIZE - len(file_transfer_header) - 1)

            idx = self.find_nearest_line_break(chunk)
            buffer = chunk[idx:]
            chunk = chunk[:idx]
            # 4 bytes for length + 1 byte for delimiter
            lenght_sent = len(chunk) + len(file_transfer_header) + 1
            logging.info("Sent %d bytes of %s to server",
                         lenght_sent, file_name)

            write_to_socket(self.client_socket,
                            file_transfer_header + b"|" + chunk)

            while self.running and self.client_socket and lenght_sent < total_msg_bytes:
                chunk = buffer
                buffer = b""
                chunk += file.read(BATCH_SIZE - 4 - len(chunk))

                idx = self.find_nearest_line_break(chunk)
                buffer = chunk[idx:]
                chunk = chunk[:idx]
                lenght_sent += len(chunk)
                write_to_socket(self.client_socket, chunk)

                logging.info(
                    f"Sent {lenght_sent} bytes of {file_name} to server")

        logging.info(f"Sent {file_name} to server")

    def find_nearest_line_break(self, chunk: bytes) -> int:
        for i in range(len(chunk)-1, -1, -1):
            if chunk[i] == 10:  # \n in ASCII
                return i + 1
        return len(chunk)

    def __send_credits_in_chunks(self, file_path: str):
        total_bytes_sent = 0
        file_size = os.path.getsize(file_path)
        file_transfer_header = len(str(file_size)) + 1 + file_size
        chunk = (f"{file_transfer_header}|").encode('utf-8')
        file_name = file_path.split("/")[-1]
        logging.info(
            f"Started sending {file_transfer_header}B to server for {file_name}")

        j = 0
        with open(file_path, 'r') as file:
            for csv_row in file:
                if j != 0:
                    # length of row that are not the header
                    csv_row_size_bytes = f"{len(csv_row)}|".encode('utf-8')
                    chunk += csv_row_size_bytes

                j += 1
                i = 0
                csv_row_decoded = csv_row.encode('utf-8')

                while i < len(csv_row_decoded):
                    row_info = csv_row_decoded[i:i + BATCH_SIZE - len(chunk)]
                    total_bytes_sent += len(row_info)
                    chunk += row_info
                    i += len(row_info)

                    write_to_socket(self.client_socket, chunk)
                    chunk = b""
                    logging.info(
                        f"Sent {total_bytes_sent} bytes of {file_name} to server")

        logging.info(f"Sent {file_name} to server")
