import logging
from common.communication import Socket
from common.fileProcessor import MoviesProcessor, CreditsProcessor, RatingsProcessor
from .worker import Worker, WorkerConfig

EOF = "EOF"

MOVIES_ROUTING_KEY_INDEX = 0
RATINGS_ROUTING_KEY_INDEX = 1
CREDITS_ROUTING_KEY_INDEX = 2


class Client:
    def __init__(self, socket: Socket, config: WorkerConfig):
        self.client_socket = socket
        self.worker = Worker(config)
        self.batch_processor = MoviesProcessor()
        try:
            self.worker.init_senders()
            self.worker.init_receiver()
        except Exception as e:
            logging.error(f"Error initializing worker: {e}")
            return e

    def close(self):
        self.client_socket.close()
        # self.worker.close_worker()

    def read(self):
        return self.client_socket.read()

    def send(self, data: str):
        return self.client_socket.send(data)

    def send_message(self, data: str):
        exchange = None
        routing_key = None

        # TODO: we have only one routing key now for working queue, change worker with it
        if type(self.batch_processor) == MoviesProcessor:
            routing_key = self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEY_INDEX]
            exchange = self.worker.exchange.name
        elif type(self.batch_processor) == CreditsProcessor:
            routing_key = self.worker.exchange.output_routing_keys[CREDITS_ROUTING_KEY_INDEX]
            exchange = self.worker.exchange.name
        elif type(self.batch_processor) == RatingsProcessor:
            routing_key = self.worker.exchange.output_routing_keys[RATINGS_ROUTING_KEY_INDEX]
            exchange = self.worker.exchange.name
        else:
            logging.error(f"Invalid batch processor: {type(self.batch_processor)}")
            return

        if not routing_key or not exchange:
            raise ValueError("Routing key or exchange is not set.")

        self.worker.send_message(data, routing_key)

    def set_next_processor(self):
        if type(self.batch_processor) == MoviesProcessor:
            self.batch_processor = CreditsProcessor()
        elif type(self.batch_processor) == CreditsProcessor:
            self.batch_processor = RatingsProcessor()
        else:
            self.batch_processor = None

    def send_message_to_workers(self):
        self.send_message(self.batch_processor.get_processed_batch())

    def send_eof(self):
        self.send_message(EOF)
        
    def send_all_eof(self):
        while self.batch_processor:
            self.send_eof()
            self.set_next_processor()

    def receive_first_chunck(self):
        if self.client_socket is None:
            raise ValueError("Client socket is not connected.")
        bytes_read, chunck_received = self.read()
        return self.batch_processor.process_first_batch(bytes_read, chunck_received)
