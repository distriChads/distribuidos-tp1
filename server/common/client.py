import logging
from common.fileProcessor import MoviesProcessor, CreditsProcessor, RatingsProcessor
from .worker import Worker


class Client:
    def __init__(self, socket, config):
        self.client_socket = socket
        self.worker = Worker(config)
        self.queue_number = 0
        self.batch_processor = MoviesProcessor()
        try:
            self.worker.init_senders()
            self.worker.init_receiver()
        except Exception as e:
            logging.error(f"Error initializing worker: {e}")
            return e

    def read(self):
        return self.client_socket.read()

    def send(self, data):
        return self.client_socket.send(data)

    def queue_to_send(self, routing_keys_len):
        self.queue_number = (
            self.queue_number + 1) % routing_keys_len
        return self.queue_number

    def send_to_joiner_worker(self, data_send, routing_keys, exchange):
        if data_send == "EOF":
            for routing_key in routing_keys:
                self.worker.send_message(data_send, routing_key, exchange)
        else:
            id = data_send.split("|")[0]
            hash = int(id) % len(routing_keys)
            routing_key = routing_keys[hash]
            self.worker.send_message(data_send, routing_key, exchange)

    def send_message(self, data_send):
        if type(self.batch_processor) == MoviesProcessor:
            queue_to_send = self.queue_to_send(
                len(self.worker.output_exchange1.routing_keys))
            exchange = self.worker.output_exchange1
            routing_key = self.worker.output_exchange1.routing_keys[queue_to_send]
            if data_send == "EOF":
                for routing_key in self.worker.output_exchange1.routing_keys:
                    self.worker.send_message(data_send, routing_key, exchange)
                logging.info("EOF sent")
            self.worker.send_message(data_send, routing_key, exchange)

        elif type(self.batch_processor) == CreditsProcessor:
            self.send_to_joiner_worker(
                data_send, self.worker.output_exchange2.routing_keys, self.worker.output_exchange2)

        else:  # RatingsProcessor
            self.send_to_joiner_worker(
                data_send, self.worker.output_exchange3.routing_keys, self.worker.output_exchange3)

    def set_next_processor(self):
        if type(self.batch_processor) == MoviesProcessor:
            self.batch_processor = CreditsProcessor()
        elif type(self.batch_processor) == CreditsProcessor:
            self.batch_processor = RatingsProcessor()

    def send_all_batch_data(self):
        self.send_message(self.batch_processor.get_all_data())

    def send_eof(self):
        self.send_message("EOF")

    def send_batch_if_threshold_reached(self):
        if self.batch_processor.ready_to_send():
            data = self.batch_processor.get_processed_batch()
            self.send_message(data)

    def receive_first_chunck(self):
        if self.client_socket is None:
            raise ValueError("Client socket is not connected.")
        bytes_read, chunck_received = self.read()
        return self.batch_processor.process_first_batch(bytes_read, chunck_received)
