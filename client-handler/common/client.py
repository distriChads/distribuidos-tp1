import logging
from common.communication import Socket
from common.fileProcessor import MoviesProcessor, CreditsProcessor, RatingsProcessor
from .worker import Worker, WorkerConfig

EOF = "EOF"

MOVIES_ROUTING_KEYS = ["filter_arg", "filter_one_country"]
CREDITS_ROUTING_KEYS = "join_movies_credits"
RATINGS_ROUTING_KEYS = "join_movies_rating"


class Client:
    def __init__(self, socket: Socket, config: WorkerConfig):
        self.client_socket = socket
        self.worker = Worker(config)
        logging.info("Initializing client with output routing keys: %s",
                     self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEYS[0]])
        filter_arg_positions = len(
            self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEYS[0]])
        filter_only_one_country_positions = len(
            self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEYS[1]])
        positions_for_hasher = {
            MOVIES_ROUTING_KEYS[0]: filter_arg_positions,
            MOVIES_ROUTING_KEYS[1]: filter_only_one_country_positions,
        }
        logging.info(
            "Initializing batch processor with positions: %s", positions_for_hasher)
        self.batch_processor = MoviesProcessor(positions_for_hasher)
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

    def send_message(self, data_list: list[list[str]]):
        routing_keys = []

        # TODO: we have only one routing key now for working queue, change worker with it
        if type(self.batch_processor) == MoviesProcessor:
            routing_keys_filter_arg = self.worker.exchange.output_routing_keys[
                MOVIES_ROUTING_KEYS[0]]
            routing_keys_filter_one_country = self.worker.exchange.output_routing_keys[
                MOVIES_ROUTING_KEYS[1]]

            data_filter_arg = data_list[0]
            data_filter_one_country = data_list[1]

            self.send_messages_with_routing(
                routing_keys_filter_arg, data_filter_arg)
            self.send_messages_with_routing(
                routing_keys_filter_one_country, data_filter_one_country)

        elif type(self.batch_processor) == CreditsProcessor:
            routing_keys = self.worker.exchange.output_routing_keys[CREDITS_ROUTING_KEYS]
            data_to_send = data_list[0]

            self.send_messages_with_routing(routing_keys, data_to_send)

        elif type(self.batch_processor) == RatingsProcessor:
            routing_keys = self.worker.exchange.output_routing_keys[RATINGS_ROUTING_KEYS]
            data_to_send = data_list[0]

            self.send_messages_with_routing(routing_keys, data_to_send)

        else:
            logging.error(
                f"Invalid batch processor: {type(self.batch_processor)}")
            return

    def send_messages_with_routing(self, routing_keys_list, messages_to_send):
        for i in range(len(routing_keys_list)):
            routing_key = routing_keys_list[i]
            data_to_send = messages_to_send[i]
            if not data_to_send:
                continue
            self.worker.send_message(data_to_send, routing_key)

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
                self.worker.exchange.output_routing_keys[MOVIES_ROUTING_KEYS[1]]
        elif type(self.batch_processor) == CreditsProcessor:
            routing_keys = self.worker.exchange.output_routing_keys[CREDITS_ROUTING_KEYS]
        elif type(self.batch_processor) == RatingsProcessor:
            routing_keys = self.worker.exchange.output_routing_keys[RATINGS_ROUTING_KEYS]
        else:
            logging.error(
                f"Invalid batch processor: {type(self.batch_processor)}")
            return

        for routing_key in routing_keys:
            self.worker.send_message(EOF, routing_key)

    def send_all_eof(self):
        while self.batch_processor:
            self.send_eof()
            self.set_next_processor()

    def receive_first_chunck(self):
        if self.client_socket is None:
            raise ValueError("Client socket is not connected.")
        bytes_read, chunck_received = self.read()
        return self.batch_processor.process_first_batch(bytes_read, chunck_received)
