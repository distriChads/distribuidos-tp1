import logging
from time import sleep
from common.communication import Socket
from common.fileProcessor import MoviesProcessor, CreditsProcessor, RatingsProcessor
from .worker import Worker, WorkerConfig
import uuid
EOF = "EOF"

MOVIES_ROUTING_KEYS = ["filter_arg", "filter_one_country"]
CREDITS_ROUTING_KEYS = "join_movies_credits"
RATINGS_ROUTING_KEYS = "join_movies_rating"
ML_ROUTING_KEYS = "machine_learning"


class Client:
    def __init__(self, socket: Socket, config: WorkerConfig):
        self.client_socket = socket
        self.worker = Worker(config)
        self.client_id = str(uuid.uuid4())
        logging.info("Initializing client with output routing keys: %s",
                     self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEYS[0]])

        filter_arg_positions = len(
            self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEYS[0]])
        filter_only_one_country_positions = len(
            self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEYS[1]])
        ml_positions = len(
            self.worker.exchange.output_routing_keys[ML_ROUTING_KEYS])

        positions_for_hasher = {
            MOVIES_ROUTING_KEYS[0]: filter_arg_positions,
            MOVIES_ROUTING_KEYS[1]: filter_only_one_country_positions,
            ML_ROUTING_KEYS: ml_positions,
        }
        self.batch_processor = MoviesProcessor(positions_for_hasher)

    def init_worker(self) -> None:
        try:
            self.worker.init_senders()
            self.worker.init_receiver()
        except Exception as e:
            logging.error(f"Error initializing worker: {e}")
            return e

    def close(self):
        self.client_socket.close()
        self.worker.close_worker()

    def read(self):
        return self.client_socket.read()

    def send(self, data: str):
        return self.client_socket.send(data)

    def send_message(self, data_list: dict[str, dict[int, str]]):
        for routing_key, dict_positions in data_list.items():
            routing_keys = self.worker.exchange.output_routing_keys[routing_key]
            if not dict_positions:
                continue
            for position, data in dict_positions.items():
                if not data:
                    continue
                self.worker.send_message(data, routing_keys[position], self.client_id)

    def set_next_processor(self):
        if type(self.batch_processor) == MoviesProcessor:
            join_movies_credits_positions = len(
                self.worker.exchange.output_routing_keys[CREDITS_ROUTING_KEYS])
            positions_for_hasher = {
                CREDITS_ROUTING_KEYS: join_movies_credits_positions,
            }
            self.batch_processor = CreditsProcessor(positions_for_hasher)
        elif type(self.batch_processor) == CreditsProcessor:
            joiin_movies_rating_positions = len(
                self.worker.exchange.output_routing_keys[RATINGS_ROUTING_KEYS])
            positions_for_hasher = {
                RATINGS_ROUTING_KEYS: joiin_movies_rating_positions,
            }
            self.batch_processor = RatingsProcessor(positions_for_hasher)
        else:
            self.batch_processor = None

    def send_message_to_workers(self):
        self.send_message(self.batch_processor.get_processed_batch())

    def send_eof(self):
        routing_keys = []
        if type(self.batch_processor) == MoviesProcessor:
            routing_keys = self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEYS[0]] + \
                self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEYS[1]] + \
                self.worker.exchange.output_routing_keys[ML_ROUTING_KEYS]
        elif type(self.batch_processor) == CreditsProcessor:
            routing_keys = self.worker.exchange.output_routing_keys[CREDITS_ROUTING_KEYS]
        elif type(self.batch_processor) == RatingsProcessor:
            routing_keys = self.worker.exchange.output_routing_keys[RATINGS_ROUTING_KEYS]
        else:
            logging.error(
                f"Invalid batch processor: {type(self.batch_processor)}")
            return

        for routing_key in routing_keys:
            self.worker.send_message(EOF, routing_key, self.client_id)

    def send_all_eof(self):
        while self.batch_processor:
            self.send_eof()
            self.set_next_processor()

    def receive_first_chunk(self):
        if self.client_socket is None:
            raise ValueError("Client socket is not connected.")
        bytes_read, chunck_received = self.read()
        return self.batch_processor.process_first_batch(bytes_read, chunck_received)
    
class StaleClient(Client):
    def __init__(self, client_id: str, config: WorkerConfig):
        super().__init__(None, config)
        self.client_id = client_id
        self.init_worker()
        
    def close(self):
        self.worker.close_worker()
