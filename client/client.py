import logging
import os
import signal
import socket
import time
import threading
from types import FrameType
from typing import Optional

from communication import Socket


BATCH_SIZE = (1024*8) - 4  # 4 bytes for the length of the message
EOF = "EOF"


class Client:
    """
    Client class to handle query requests.
    """

    def __init__(self,
            client_handler_address: str,
            client_handler_port: int,
            dataset_path: str,
            results_path: str,
            max_retries: int,
            retry_delay: int,
            backoff_factor: int,
        ):
        """
        Initializes the client.
        :param client_handler_address: address of the client handler
        :param client_handler_port: port of the client handler
        :param dataset_path: path to the datasets for the queries
        :param results_path: path to save the results to
        :param max_retries: maximum number of retries to connect to the client handler
        :param retry_delay: delay between retries
        :param backoff_factor: backoff factor for the retries
        """
        self.client_handler_address = client_handler_address
        self.client_handler_port = client_handler_port
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket = Socket(client_socket)

        self.movies_path = os.path.join(dataset_path, "movies_metadata.csv")
        self.credits_path = os.path.join(dataset_path, "credits.csv")
        self.ratings_path = os.path.join(dataset_path, "ratings.csv")

        self.running = True
        self.results_thread = None
        self.client_id = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_factor = backoff_factor
        self.results_path = results_path
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        """
        Cleans up client resources before shutting down.
        """
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
        """
        Connects to the client handler, with retries.
        """
        attempts = 0
        while attempts < self.max_retries and self.running:
            try:
                self.client_socket.sock.connect(
                    (self.client_handler_address, self.client_handler_port))
                break
            except Exception as e:
                logging.error(f"Error connecting to client handler: {e}")
                attempts += 1
                time.sleep(self.retry_delay * self.backoff_factor * attempts)
        logging.info(f"Connected to client handler")

    def run(self):
        """
        Main Client loop. 
        1. Connects to client handler and waits for results in the background.
        2. Sends the files to the client handler in chunks.
        3. Waits for all results to arrive.
        4. Orders the queries alphabetically.
        """
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
        self.order_queries()
        self.__graceful_shutdown_handler()

    def __wait_for_results(self):
        """
        Main loop for results handling.
        Waits for results from the client handler and writes them to the appropiate results file.
        """
        while self.running:
            try:
                self.client_socket.sock.settimeout(4)
                _bytes_read, result = self.client_socket.read()

                if not result or result == EOF:
                    logging.info(
                        "All queries have been processed - Shutting down")
                    break

                client_id = result.split("/")[0]
                query_id = result.split("/")[1]
                result = result.split("/")[2]

                if not self.client_id:
                    print(f"\n-----------Client ID: {client_id}-----------\n")
                    self.client_id = client_id

                self.__write_down_in_file(client_id, f"{query_id}.txt", result)
            except socket.timeout:
                if not self.running:
                    logging.info("Socket timeout, shutting down")
                    break
            except socket.error as e:
                logging.info(f"Closing socket")
                break

    def __write_down_in_file(self, dir_name: str, file_name: str, result: str):
        dir_name = os.path.join("results", dir_name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        file_path = os.path.join(dir_name, file_name)

        with open(file_path, "a") as file:
            file.write(result)

    def __send_file_in_chunks(self, file_path: str):
        """
        Send a file in chunks to the client handler
        :param file_path: path to the file
        """
        file_size = os.path.getsize(file_path)
        total_msg_bytes = len(str(file_size)) + 1 + file_size
        file_transfer_header = str(total_msg_bytes).encode('utf-8')
        file_name = file_path.split("/")[-1]
        logging.info(
            f"Started sending {total_msg_bytes}B to client handler for {file_name}")

        buffer = b""
        with open(file_path, 'rb') as file:
            # 4 bytes for length + 1 byte for delimiter
            chunk = file.read(BATCH_SIZE - len(file_transfer_header) - 1)

            idx = self.find_nearest_line_break(chunk)
            buffer = chunk[idx:]
            chunk = chunk[:idx]
            # 4 bytes for length + 1 byte for delimiter
            lenght_sent = len(chunk) + len(file_transfer_header) + 1
            logging.info("Sent %d bytes of %s to client handler",
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
                    f"\rSent {percent_bytes_sent}% of {file_name} to client handler", end="")
        print()
        logging.info(f"Sent {file_name} to client handler")

    def find_nearest_line_break(self, chunk: bytes) -> int:
        """
        Finds the line break ('\n') nearest to the end of chunk.
        :param chunk: array of bytes
        :return: index of the nearest line break
        """
        for i in range(len(chunk)-1, -1, -1):
            if chunk[i] == 10:  # \n in ASCII
                return i + 1
        return len(chunk)

    def __send_credits_in_chunks(self, file_path: str):
        """
        Send the credits file in chunks to the client handler
        :param file_path: path to the file
        """
        total_bytes_sent = 0
        file_size = os.path.getsize(file_path)
        file_transfer_header = len(str(file_size)) + 1 + file_size
        chunk = (f"{file_transfer_header}|").encode('utf-8')
        file_name = file_path.split("/")[-1]
        logging.info(
            f"Started sending {file_transfer_header}B to client handler for {file_name}")

        j = 0
        with open(file_path, 'r', encoding="utf-8") as file:
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
                        f"\rSent {percent_bytes_sent}% of {file_name} to client handler", end="")
        print()
        logging.info(f"Sent {file_name} to client handler")

    def order_queries(self) -> None:
        """
        Orders queries 1 and 5.
        Query 1 is ordered alphabetically.
        Query 5 is ordered by positive rating first.
        Queries 2, 3 and 4 come ordered from the client handler since they have inherent order.
        """
        if not self.client_id:
            logging.error("Client ID was never received")
            return
        if not os.path.exists(os.path.join("results", self.client_id)):
            logging.error(f"No results found for client {self.client_id}")
            return

        q1_path = os.path.join("results", self.client_id, "query1.txt")
        with open(q1_path, "r") as file:
            q1 = file.readlines()
        q1 = [line.strip() for line in q1]
        q1.sort()
        q1_aux_path = os.path.join("results", self.client_id, "query1-aux.txt")
        with open(q1_aux_path, "w") as file:
            file.write("\n".join(q1) + "\n")
        os.rename(q1_aux_path, q1_path)

        q5_path = os.path.join("results", self.client_id, "query5.txt")
        with open(q5_path, "r") as file:
            q5 = file.readlines()
        q5 = [line.strip() for line in q5]
        q5.sort(reverse=True)  # postive first
        q5_aux_path = os.path.join("results", self.client_id, "query5-aux.txt")
        with open(q5_aux_path, "w") as file:
            file.write("\n".join(q5) + "\n")
        os.rename(q5_aux_path, q5_path)
