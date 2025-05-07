import logging
from common.fileProcessor import MoviesProcessor, CreditsProcessor, RatingsProcessor
from .worker import Worker

EOF = "EOF"


class Client:
    def __init__(self, socket, config):
        self.client_socket = socket
        self.worker = Worker(config)
        self.queue_number = 0
        self.batch_processor = MoviesProcessor(
            len(self.worker.output_exchange1.routing_keys))
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

    def send_to_joiner_worker(self, data_list, exchange):
        if data_list[0] == EOF:
            self.send_eof_to_exchange(data_list[0], exchange)
        for hash in range(len(data_list)):
            data = data_list[hash]
            if len(data) != 0:
                routing_key = exchange.routing_keys[hash]
                self.worker.send_message(data, routing_key, exchange)

    def send_message(self, data_list):
        send_eof = False

        if data_list[0] == EOF:
            send_eof = True

        if type(self.batch_processor) == MoviesProcessor:
            if send_eof:
                self.send_eof_to_exchange(
                    data_list[0], self.worker.output_exchange1)

            data_send = "".join(data_list)
            queue_to_send = self.queue_to_send(
                len(self.worker.output_exchange1.routing_keys))
            routing_key = self.worker.output_exchange1.routing_keys[queue_to_send]
            self.worker.send_message(
                data_send, routing_key, self.worker.output_exchange1)

        elif type(self.batch_processor) == CreditsProcessor:
            self.send_to_joiner_worker(data_list, self.worker.output_exchange2)

        else:  # RatingsProcessor
            self.send_to_joiner_worker(data_list, self.worker.output_exchange3)

    def send_eof_to_exchange(self, data, exchange):
        for routing_key in exchange.routing_keys:
            self.worker.send_message(data, routing_key, exchange)

    def set_next_processor(self):
        if type(self.batch_processor) == MoviesProcessor:
            self.batch_processor = CreditsProcessor(
                len(self.worker.output_exchange2.routing_keys))
        elif type(self.batch_processor) == CreditsProcessor:
            self.batch_processor = RatingsProcessor(
                len(self.worker.output_exchange3.routing_keys))

    def send_all_batch_data(self):
        self.send_message(self.batch_processor.get_all_data_from_hash())

    def send_message_to_workers(self):
        data_list = self.batch_processor.get_processed_batch()
        self.send_message(data_list)

    def send_eof(self):
        data_list = [EOF]
        self.send_message(data_list)

    def receive_first_chunck(self):
        if self.client_socket is None:
            raise ValueError("Client socket is not connected.")
        bytes_read, chunck_received = self.read()
        return self.batch_processor.process_first_batch(bytes_read, chunck_received)
