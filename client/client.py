import logging
import os
import signal
import socket
import threading
from types import FrameType
from typing import Optional

from communication import Socket


BATCH_SIZE = (1024*8) - 4  # 4 bytes for the length of the message


class Client:
    def __init__(self, server_address: str, server_port: int, storage_path: str):
        self.server_address = server_address
        self.server_port = server_port
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket = Socket(client_socket)

        self.movies_path = os.path.join(storage_path, "movies_metadata.csv")
        self.credits_path = os.path.join(storage_path, "credits.csv")
        self.ratings_path = os.path.join(storage_path, "ratings.csv")

        self.running = True
        self.results_thread = None
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        self.running = False
        self.client_socket.sock.close()
        try:
            if self.results_thread:
                print("joining thread")
                self.results_thread.join(timeout=2)
        except Exception as e:
            logging.error(f"Error joining thread: {e}")
        print("thread joined")
        logging.info("Client socket closed")

    def __connect(self):
        self.client_socket.sock.connect(
            (self.server_address, self.server_port))
        logging.info(f"Connected to server")

    def run(self):
        self.__connect()
        self.results_thread = threading.Thread(target=self.__wait_for_results)
        self.results_thread.start()

        try:
            self.__send_file_in_chunks(self.movies_path)
            self.__send_credits_in_chunks(self.credits_path)
            self.__send_file_in_chunks(self.ratings_path)

        except Exception as e:
            logging.error(f"Error: {e}")

        self.results_thread.join()
        self.__graceful_shutdown_handler()

    def __wait_for_results(self):
        while self.running:
            try:
                self.client_socket.sock.settimeout(4)
                _bytes_read, result = self.client_socket.read()
                if not result:
                    break
                query_id = result.split("-")[0]
                result = result.split("-")[1]
                self.__write_down_in_file(query_id, result)
            except socket.timeout:
                if not self.running:
                    logging.info("Socket timeout, shutting down")
                    break
                else:
                    logging.info("Socket timeout, waiting for results")
            except socket.error as e:
                logging.info(f"Closing socket")
                break

    def __write_down_in_file(self, file_name: str, result: str):
        with open(file_name, "a") as file:
            file.write(result)

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

            self.client_socket.send(
                file_transfer_header + b"|" + chunk)

            while self.running and self.client_socket and lenght_sent < total_msg_bytes:
                chunk = buffer
                buffer = b""
                chunk += file.read(BATCH_SIZE - 4 - len(chunk))

                idx = self.find_nearest_line_break(chunk)
                buffer = chunk[idx:]
                chunk = chunk[:idx]
                lenght_sent += len(chunk)
                self.client_socket.send(chunk)

                percent_bytes_sent = (lenght_sent / file_size) * 100
                percent_bytes_sent = f"{percent_bytes_sent:05.2f}"
                print(
                    f"\rSent {percent_bytes_sent}% of {file_name} to server", end="")
        print()
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
                if not self.running:
                    break
                if j != 0:
                    # length of row that are not the header
                    csv_row_size_bytes = f"{len(csv_row)}|".encode('utf-8')
                    chunk += csv_row_size_bytes

                j += 1
                i = 0
                csv_row_decoded = csv_row.encode('utf-8')

                while i < len(csv_row_decoded) and self.running:
                    row_info = csv_row_decoded[i:i + BATCH_SIZE - len(chunk)]
                    total_bytes_sent += len(row_info)
                    chunk += row_info
                    i += len(row_info)

                    self.client_socket.send(chunk)
                    chunk = b""

                    percent_bytes_sent = round(
                        (total_bytes_sent / file_size) * 100, 2)
                    percent_bytes_sent = f"{percent_bytes_sent:05.2f}"
                    print(
                        f"\rSent {percent_bytes_sent}% of {file_name} to server", end="")
        print()
        logging.info(f"Sent {file_name} to server")
