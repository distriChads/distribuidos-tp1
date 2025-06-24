# from transformers import pipeline
from .worker import Worker, WorkerConfig, MESSAGE_SEPARATOR, MESSAGE_EOF
from textblob import TextBlob
from .hasher import HasherContainer

import logging
import threading
import queue
import uuid

log = logging.getLogger("machine_learning")

FIELDS_COUNT = 8
BATCH_SIZE = 20

MOVIE_ID_INDEX = 0
OVERVIEW_FIELD_INDEX = 6
BUDGET_INDEX = 5
REVENUE_INDEX = 7

OUTPUT_KEY = "overview_average"


class MachineLearningConfig(WorkerConfig):
    pass


class MachineLearning:
    def __init__(self, config: MachineLearningConfig):
        log.info(f"NewMachineLearning: {config.__dict__}")
        self.worker = Worker(config)
        # self.sentiment_analyzer = pipeline(
        #     'sentiment-analysis',
        #     model='distilbert-base-uncased-finetuned-sst-2-english',
        #     batch_size=BATCH_SIZE,
        #     truncation=True,
        # )

        dict_for_hasher = {
            OUTPUT_KEY: len(config.exchange.output_routing_keys[OUTPUT_KEY]),
        }
        self.buffer = HasherContainer(dict_for_hasher)
        self.batches_per_client: dict[str, list[list[str]]] = {}
        self.revenue_budget_zero_count_per_client: dict[str, int] = {}
        self.movies_processed_per_client: dict[str, int] = {}

        try:
            self.worker.init_senders()
            self.worker.init_receiver()
        except Exception as e:
            log.error(f"Error initializing worker: {e}")
            return e

    def __shutdown(self):
        log.info("Shutting down MachineLearning worker")
        self.worker.close_worker()        

    def __process_batch(self, client_id: str):
        sentiments: list[dict[str, str]] = []
        movies_batch = self.batches_per_client[client_id]

        log.debug(
            f"Amount of 0 on revenue or budget: {self.revenue_budget_zero_count_per_client[client_id]}")
        self.revenue_budget_zero_count_per_client[client_id] = 0

        overviews = [movie[OVERVIEW_FIELD_INDEX] for movie in movies_batch]

        try:
            # sentiments = self.sentiment_analyzer(overviews)
            self._analyze_sentiment(overviews, sentiments)
        except Exception as e:
            log.error(f"Error in batch sentiment analysis: {e}")
            return

        i = 0
        for movie_components, sentiment in zip(movies_batch, sentiments):
            self.__add_message_to_buffer(sentiment["label"], movie_components)
            i += 1

        self.movies_processed_per_client[client_id] += i + \
            self.revenue_budget_zero_count_per_client[client_id]
        log.info(
            f"Processed {self.movies_processed_per_client[client_id]} movies")

    def _analyze_sentiment(self, overviews: list[str], sentiments: list[dict[str, str]]):
        for overview in overviews:
            blob = TextBlob(overview)
            sentiment = blob.sentiment
            if sentiment.polarity >= 0:
                sentiments.append({"label": "POSITIVE"})
            elif sentiment.polarity < 0:
                sentiments.append({"label": "NEGATIVE"})

    def __add_message_to_buffer(self, sentiment: str, parts: list[str]):
        result = MESSAGE_SEPARATOR.join(
            [parts[MOVIE_ID_INDEX], sentiment, parts[BUDGET_INDEX], parts[REVENUE_INDEX]])
        result += "\n"
        if not parts[MOVIE_ID_INDEX].isdigit():
            log.warning(f"Invalid movie ID: {parts[MOVIE_ID_INDEX]}")
            return
        movie_id = int(parts[MOVIE_ID_INDEX])
        self.buffer.append_to_node(movie_id, result)

    def run_worker(self):
        log.info("Starting MachineLearning worker")
        try:
            for method_frame, _properties, body in self.worker.received_messages():
                raw_msg = body.decode("utf-8")
                delivery_tag = method_frame.delivery_tag
                raw_msg = raw_msg.strip()
                if not raw_msg:
                    self.worker.send_ack(delivery_tag)
                    continue

                client_id, message_id, message = raw_msg.split(MESSAGE_SEPARATOR, 2)
                if client_id not in self.batches_per_client:
                    self.batches_per_client[client_id] = []
                    self.movies_processed_per_client[client_id] = 0

                if message == MESSAGE_EOF:
                    self._process_remaining_messages(message, client_id, message_id)
                    self.worker.send_ack(delivery_tag)
                    continue

                self.use_all_messages_up(client_id, message)
                self.worker.send_ack(delivery_tag)

        except Exception as e:
            log.error(f"Error during message processing: {e}")
            self.__shutdown()
            return e

    def use_all_messages_up(self, client_id: str, message: str):
        messages = message.split("\n")
        messages, amount_of_0 = self._filter_wrong_messages(
            messages, client_id)
        self.revenue_budget_zero_count_per_client[client_id] = (
            self.revenue_budget_zero_count_per_client.get(
                client_id, 0) + amount_of_0
        )

        pending_movies_count = len(self.batches_per_client[client_id])
        necessary_movies_count = BATCH_SIZE - pending_movies_count
        self.batches_per_client[client_id].extend(
            messages[:necessary_movies_count])
        messages = messages[necessary_movies_count:]

        while len(self.batches_per_client[client_id]) >= BATCH_SIZE:
            self.__process_and_send_batch(client_id)
            self.batches_per_client[client_id] = messages[:BATCH_SIZE]
            messages = messages[BATCH_SIZE:]

    def _filter_wrong_messages(self, messages: list[str], client_id: str) -> tuple[list[list[str]], int]:
        valid_movies: list[list[str]] = []
        revenue_0_in_batch = 0

        for message in messages:
            movies_components = message.split(MESSAGE_SEPARATOR)

            if len(movies_components) < FIELDS_COUNT:
                log.warning(f"Incomplete message skipped: {movies_components}")
                continue

            if movies_components[BUDGET_INDEX] == "0" or movies_components[REVENUE_INDEX] == "0":
                self.movies_processed_per_client[client_id] += 1
                revenue_0_in_batch += 1
                continue

            valid_movies.append(movies_components)

        return valid_movies, revenue_0_in_batch

    def _process_remaining_messages(self, message: str, client_id: str, eof_id: str):
        if len(self.batches_per_client[client_id]) > 0:
            self.__process_and_send_batch(client_id)
            del self.batches_per_client[client_id]

        for routing_key in self.worker.exchange.output_routing_keys[OUTPUT_KEY]:
            self.worker.send_message(
                f"{client_id}{MESSAGE_SEPARATOR}{eof_id}{MESSAGE_SEPARATOR}{message}", routing_key)
        log.info("Sent EOF to all routing keys")

    def __process_and_send_batch(self, client_id: str):
        self.__process_batch(client_id)
        messages = self.buffer.get_buffers()
        dict_index_data = messages[OUTPUT_KEY]
        routing_keys = self.worker.exchange.output_routing_keys[OUTPUT_KEY]

        for index, message in dict_index_data.items():
            routing_key = routing_keys[index]
            identifier = str(uuid.uuid4())
            result = f"{client_id}{MESSAGE_SEPARATOR}{identifier}{MESSAGE_SEPARATOR}{message}"
            self.worker.send_message(result, routing_key)
