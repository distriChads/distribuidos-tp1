import logging
import signal
import socket
import os
import threading
from types import FrameType
from typing import Optional
from .worker import Worker, WorkerConfig
from .client import Client, StaleClient
from collections import defaultdict
from common.communication import Socket
from common.stateManager import StateManager

FILES_TO_RECEIVE = 3
EOF = "EOF"


class ClientHandlerConfig(WorkerConfig):
    pass


class ClientHandler:
    def __init__(self,
                 port: int,
                 client_handler_config: ClientHandlerConfig,
                 listen_backlog: int,
                 eof_expected: int,
                 state_file_path: str,
                 ):
        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGINT, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGABRT, self.__graceful_shutdown_handler)

        # Initialize instance variables
        self._shutdown = threading.Event()
        self.client_handler_config = client_handler_config
        self.eof_expected = eof_expected
        self.eof_per_client: dict[str, int] = {}  # TODO: REMOVE
        self.clients_lock = threading.Lock()
        self.clients: dict[str, Client] = {}
        self.client_threads_lock = threading.Lock()
        self.client_threads: dict[str, threading.Thread] = {}

        # Initialize middleware worker
        self.worker = Worker(client_handler_config)
        try:
            self.worker.init_receiver()
        except Exception as e:
            logging.error(f"Error initializing worker: {e}")
            raise e

        # Create listener socket for clients
        self._cli_hand_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        self._cli_hand_socket.bind(('', port))
        self._cli_hand_socket.listen(listen_backlog)
        
        # Initialize state manager and clean stale clients
        self.state_manager = StateManager(state_file_path)

    def __graceful_shutdown_handler(self, signum: Optional[int] = None, frame: Optional[FrameType] = None):
        """
        Handles resource cleanup and shutdown of the client handler.
        """
        # ===== TEST: ungraceful shutdown on client handler =====
        if signum == signal.SIGABRT:
            logging.critical("Client handler blew up")
            os._exit(1)
        # ===== TEST: ungraceful shutdown on client handler =====
        
        self._shutdown.set()
        self.worker.close_worker()
        self._cli_hand_socket.close()
        

    def __clean_stale_clients(self) -> None:
        """
        Cleans up stale clients that were not properly closed.
        Sends EOFs for all stale clients and removes them from state.
        """
        stale_clients = self.state_manager.get_all_clients()
        if not stale_clients:
            logging.info("No stale clients found")
            return
        
        logging.info(f"Cleaning {len(stale_clients)} stale clients")
        for client_id in stale_clients:
            client = StaleClient(client_id, self.client_handler_config)
            client.send_all_eof()
            client.close()
            self.state_manager.delete_client(client_id)

    def run(self) -> None:
        """
        Main client handler loop.
        Accepts new connections, processes clients in separate threads, and manages client results in a dedicated thread.
        """
        results_thread = threading.Thread(target=self.__manage_client_results)
        results_thread.start()
        self.__clean_stale_clients()

        while not self._shutdown.is_set():
            client_socket = self.__accept_new_connection()
            if client_socket:
                client_socket = Socket(client_socket)
                client = Client(client_socket, self.client_handler_config)
                with self.clients_lock:
                    self.clients[client.client_id] = client
                client_t = threading.Thread(target=self.__receive_datasets,
                                 args=(client,))
                self.client_threads[client.client_id] = client_t
                self.state_manager.add_client(client.client_id)
                client_t.start()
        for thread in self.client_threads.values():
            thread.join()
        logging.info("All client threads joined")
        results_thread.join()
        logging.info("Results thread joined")
        self.state_manager.remove_all_clients()
        logging.info("State cleaned")
        logging.info('Client handler thread finished')

    def __join_client(self, client_id: str) -> None:
        """
        Joins a client thread.
        """
        with self.client_threads_lock:
            client_t = self.client_threads.pop(client_id)
        client_t.join()
    
    def __receive_datasets(self, client: Client) -> None:
        """
        Loop for receiving datasets from a client.
        Processes the datasets and forwards them to the message broker.
        When all datasets have been received, sends EOFs to the message broker.
        """
        client.init_worker()
        for i in range(FILES_TO_RECEIVE):

            if not self._shutdown.is_set():
                client.receive_first_chunk()
                logging.debug("Receiving file %d of size %d", i,
                              client.batch_processor.read_until)
                logging.info("Received %d bytes out of %d --- file %d",
                             client.batch_processor.bytes_read, client.batch_processor.read_until, i)

            read_all_data = False
            while not self._shutdown.is_set() and not read_all_data:
                if client.batch_processor.received_all_data():
                    read_all_data = True
                    continue
                try:
                    bytes_received, chunk_received = client.read()
                    client.batch_processor.process_batch(
                        bytes_received, chunk_received)
                    client.send_message_to_workers()
                except Exception as e:
                    logging.error(
                        f'Error processing client {client.client_id}: {e}')
                    client.send_all_eof()
                    return

            client.send_eof()

            if not self._shutdown.is_set():
                logging.info(
                    f'Received file {i} of size {client.batch_processor.read_until} - read {client.batch_processor.bytes_read} bytes')
            else:
                logging.info(
                    f'File {i} skipped for client {client.client_id} due to shutdown process')

            client.set_next_processor()

        logging.info(f'Received all files for client {client.client_id}')

    def __manage_client_results(self) -> None:
        """
        Loop for receiving query results.
        Listens for results and forwards them to the respective client.
        When a client has received all results, it closes the connection.
        """
        received_messages_id = defaultdict(list)
        for method_frame, _properties, result in self.worker.received_messages(self._shutdown):
            parts = result.split("|", 2)
            client_id, message_id, result = parts
            if message_id in received_messages_id[client_id]:
                logging.warning(
                    f"Repeated message {message_id} for client {client_id}")
                self.worker.send_ack(method_frame.delivery_tag)
                continue
            received_messages_id[client_id].append(message_id)
            query_number = method_frame.routing_key.split(".")[0]
            logging.debug(
                "Received result for client %s from worker: %s", client_id, result)

            if result == EOF or len(result) == 0:
                self.eof_per_client[client_id] = self.eof_per_client.get(
                    client_id, 0) + 1
                if self.eof_per_client[client_id] >= self.eof_expected:
                    with self.clients_lock:
                        client = self.clients.pop(client_id, None)
                        if not client:
                            logging.warning(f"EOF received for non-existent client {client_id}")
                            continue
                    logging.info(
                        f"EOF received for client {client_id} - closing connection")
                    client.send(EOF)
                    client.close()
                    self.state_manager.delete_client(client_id)
                    self.__join_client(client_id)
                    if client_id in received_messages_id:
                        del received_messages_id[client_id]
                self.worker.send_ack(method_frame.delivery_tag)
                continue
            
            self.worker.send_ack(method_frame.delivery_tag)

            result = f"{client_id}/{query_number}/{result}\n"
            with self.clients_lock:
                client = self.clients.get(client_id)
            if client:
                try:
                    client.send(result)
                except Exception as e:
                    logging.error(
                        f'Error sending result to client {client_id}: {e}')
                    continue
        logging.info('Client results manager thread finished')

    def __accept_new_connection(self) -> socket.socket:
        """
        Accepts a new connection from a client.
        Returns the connection socket.
        """
        logging.debug('In listener socket loop')
        try:
            c, addr = self._cli_hand_socket.accept()
            logging.info(f'Accepted new connection from {addr[0]}')
            return c
        except OSError:
            logging.info('Listener socket closed')
            return None
        except Exception as e:
            logging.error(f'Error accepting new connection: {e}')
            raise e
