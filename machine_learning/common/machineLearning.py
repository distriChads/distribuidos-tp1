from .worker import Worker, WorkerConfig, MESSAGE_SEPARATOR, MESSAGE_EOF
from textblob import TextBlob
from .hasher import HasherContainer
import signal
import logging
import uuid
from typing import Optional
from types import FrameType

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
    """
    Machine learning worker.
    Processes reviews from messages and produces sentiment analysis as output.
    Ignores messages with invalid movie IDs or revenue/budget set to 0.

    :param config: configuration for the machine learning worker
    """

    def __init__(self, config: MachineLearningConfig):
        log.info(f"NewMachineLearning: {config.__dict__}")
        self.worker = Worker(config)

        dict_for_hasher = {
            OUTPUT_KEY: len(config.exchange.output_routing_keys[OUTPUT_KEY]),
        }
        self.buffer = HasherContainer(dict_for_hasher)
        self.revenue_budget_zero_count_per_client: dict[str, int] = {}
        self.movies_processed_per_client: dict[str, int] = {}

        # Handle SIGINT (Ctrl+C) and SIGTERM (docker stop)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, self.__graceful_shutdown_handler)

        try:
            self.worker.init_senders()
            self.worker.init_receiver()
        except Exception as e:
            log.error(f"Error initializing worker: {e}")
            return e

    def __graceful_shutdown_handler(
        self, signum: Optional[int] = None, frame: Optional[FrameType] = None
    ):
        """
        Handles graceful shutdown and resource cleanup of the machine learning worker.
        """
        self.__shutdown()

    def __shutdown(self):
        """
        Shuts down the machine learning worker.
        """
        log.info("Shutting down MachineLearning worker")
        self.worker.close_worker()

    def __process_batch(self, messages: list[list[str]], client_id: str):
        """
        Processes a batch of messages.
        """
        sentiments: list[dict[str, str]] = []

        log.debug(
            f"Amount of 0 on revenue or budget: {self.revenue_budget_zero_count_per_client[client_id]}"
        )
        self.revenue_budget_zero_count_per_client[client_id] = 0

        overviews = [movie[OVERVIEW_FIELD_INDEX] for movie in messages]

        try:
            # sentiments = self.sentiment_analyzer(overviews)
            self._analyze_sentiment(overviews, sentiments)
        except Exception as e:
            log.error(f"Error in batch sentiment analysis: {e}")
            return

        i = 0
        for movie_components, sentiment in zip(messages, sentiments):
            self.__add_message_to_buffer(sentiment["label"], movie_components)
            i += 1

        self.movies_processed_per_client[client_id] += (
            i + self.revenue_budget_zero_count_per_client[client_id]
        )
        log.info(f"Processed {self.movies_processed_per_client[client_id]} movies")

    def _analyze_sentiment(
        self, overviews: list[str], sentiments: list[dict[str, str]]
    ):
        """
        Analyzes the sentiment of a list of overviews.
        """
        for overview in overviews:
            blob = TextBlob(overview)
            sentiment = blob.sentiment
            if sentiment.polarity >= 0:
                sentiments.append({"label": "POSITIVE"})
            elif sentiment.polarity < 0:
                sentiments.append({"label": "NEGATIVE"})

    def __add_message_to_buffer(self, sentiment: str, parts: list[str]):
        """
        Adds a message to the processed messages buffer.
        """
        result = MESSAGE_SEPARATOR.join(
            [
                parts[MOVIE_ID_INDEX],
                sentiment,
                parts[BUDGET_INDEX],
                parts[REVENUE_INDEX],
            ]
        )
        result += "\n"
        if not parts[MOVIE_ID_INDEX].isdigit():
            log.warning(f"Invalid movie ID: {parts[MOVIE_ID_INDEX]}")
            return
        movie_id = int(parts[MOVIE_ID_INDEX])
        self.buffer.append_to_node(movie_id, result)

    def run_worker(self):
        """
        Main loop for the machine learning worker.
        """
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
                if client_id not in self.movies_processed_per_client:
                    self.movies_processed_per_client[client_id] = 0

                if message == MESSAGE_EOF:
                    self.send_eof_to_all_routing_keys(client_id, message_id)
                    self.worker.send_ack(delivery_tag)
                    continue

                self.process_client_messages(client_id, message, message_id)
                self.worker.send_ack(delivery_tag)

        except Exception as e:
            log.error(f"Error during message processing: {e}")
            self.__shutdown()
            return e

    def process_client_messages(self, client_id: str, message: str, message_id: str):
        """
        Processes a client message block.
        """
        messages = message.split("\n")
        messages, amount_of_0 = self._filter_wrong_messages(messages, client_id)
        self.revenue_budget_zero_count_per_client[client_id] = (
            self.revenue_budget_zero_count_per_client.get(client_id, 0) + amount_of_0
        )

        self.__process_and_send_message(messages, client_id)

    def _filter_wrong_messages(
        self, messages: list[str], client_id: str
    ) -> tuple[list[list[str]], int]:
        """
        Filters out messages with invalid movie IDs or revenue/budget set to 0.
        """
        valid_movies: list[list[str]] = []
        revenue_0_in_batch = 0

        for message in messages:
            movies_components = message.split(MESSAGE_SEPARATOR)

            if len(movies_components) < FIELDS_COUNT:
                log.warning(f"Incomplete message skipped: {movies_components}")
                continue

            if (
                movies_components[BUDGET_INDEX] == "0"
                or movies_components[REVENUE_INDEX] == "0"
            ):
                self.movies_processed_per_client[client_id] += 1
                revenue_0_in_batch += 1
                continue

            valid_movies.append(movies_components)

        return valid_movies, revenue_0_in_batch

    def __process_and_send_message(self, messages: list[list[str]], client_id: str):
        """
        Processes a batch of messages and sends the results to the message broker.
        """
        self.__process_batch(messages, client_id)
        messages_processed = self.buffer.get_buffers()

        if not messages_processed or OUTPUT_KEY not in messages_processed:
            log.warning("No messages processed or output key not found")
            return
        dict_index_data = messages_processed[OUTPUT_KEY]
        routing_keys = self.worker.exchange.output_routing_keys[OUTPUT_KEY]

        for index, message in dict_index_data.items():
            routing_key = routing_keys[index]
            identifier = str(uuid.uuid4())
            result = f"{client_id}{MESSAGE_SEPARATOR}{identifier}{MESSAGE_SEPARATOR}{message}"
            self.worker.send_message(result, routing_key)

    def send_eof_to_all_routing_keys(self, client_id: str, message_id: str):
        """
        Sends EOF messages to all routing keys.
        """
        for routing_key in self.worker.exchange.output_routing_keys[OUTPUT_KEY]:
            eof_message = f"{client_id}{MESSAGE_SEPARATOR}{message_id}{MESSAGE_SEPARATOR}{MESSAGE_EOF}"
            self.worker.send_message(eof_message, routing_key)
        log.info(f"Sent EOF message for client {client_id}")
