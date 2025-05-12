import threading
from time import sleep
# from transformers import pipeline
from textblob import TextBlob
import logging
from .worker import Worker, WorkerConfig, MESSAGE_SEPARATOR, MESSAGE_EOF
import queue

log = logging.getLogger("machine_learning")

FIELDS_COUNT = 8
BATCH_SIZE = 20

OVERVIEW_FIELD_INDEX = 6
BUDGET_INDEX = 5
REVENUE_INDEX = 7


class MachineLearningConfig(WorkerConfig):
    pass


class MachineLearning:
    def __init__(self, config: MachineLearningConfig, output_routing_keys: list[str]):
        log.info(f"NewMachineLearning: {config.__dict__}")
        self.worker = Worker(config)
        # self.sentiment_analyzer = pipeline(
        #     'sentiment-analysis',
        #     model='distilbert-base-uncased-finetuned-sst-2-english',
        #     batch_size=BATCH_SIZE,
        #     truncation=True,
        # )

        self.output_routing_keys = output_routing_keys
        self.queue_to_send = 0
        self.batches_per_client: dict[str, list[list[str]]] = {}
        self.revenue_budget_zero_count_per_client: dict[str, int] = {}
        self.movies_processed_per_client: dict[str, int] = {}

        try:
            self.worker.init_sender()
            self.worker.init_receiver()
        except Exception as e:
            log.error(f"Error initializing worker: {e}")
            return e

        self._running = True
        self.messages_queue: queue.Queue[str] = queue.Queue()
        self.thread = threading.Thread(
            target=self.__receive_messages, daemon=True)
        self.thread.start()

    def __shutdown(self):
        log.info("Shutting down MachineLearning worker")
        self._running = False
        if self.thread.is_alive():
            self.thread.join(timeout=5)
            log.info("Receiver thread stopped")
        self.worker.close_worker()

    def __receive_messages(self):
        log.info("Starting message receiver thread")
        movies_received = 0
        try:
            for _method_frame, _properties, body in self.worker.received_messages():
                if not self._running:
                    break
                message = body.decode("utf-8")
                movies_received += 1
                self.messages_queue.put(message)
        except Exception as e:
            log.error(f"Receive thread crashed: {e}")

    def __process_batch(self, client_id: str) -> list[str]:
        sentiments: list[dict[str, str]] = []
        results: list[str] = []
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
            return results

        for movie_components, sentiment in zip(movies_batch, sentiments):
            result_msg = self.__create_message_to_send(
                sentiment["label"], movie_components)
            results.append(result_msg)

        self.movies_processed_per_client[client_id] += len(results) + \
            self.revenue_budget_zero_count_per_client[client_id]
        log.info(
            f"Processed {self.movies_processed_per_client[client_id]} movies")

        return results

    def _analyze_sentiment(self, overviews: list[str], sentiments: list[dict[str, str]]):
        for overview in overviews:
            blob = TextBlob(overview)
            sentiment = blob.sentiment
            if sentiment.polarity >= 0:
                sentiments.append({"label": "POSITIVE"})
            elif sentiment.polarity < 0:
                sentiments.append({"label": "NEGATIVE"})

    def __create_message_to_send(self, sentiment: str, parts: list[str]):
        result = MESSAGE_SEPARATOR.join(
            [sentiment, parts[BUDGET_INDEX], parts[REVENUE_INDEX]])
        return result

    def run_worker(self):
        log.info("Starting MachineLearning worker")
        try:
            while True:
                raw_msg = self.messages_queue.get()
                raw_msg = raw_msg.strip()
                if not raw_msg:
                    continue

                client_id, message = raw_msg.split(MESSAGE_SEPARATOR, 1)
                if client_id not in self.batches_per_client:
                    self.batches_per_client[client_id] = []
                    self.movies_processed_per_client[client_id] = 0

                if message == MESSAGE_EOF:
                    self._process_remaining_messages(raw_msg, client_id)
                    continue

                self.use_all_messages_up(client_id, message)

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

    def _process_remaining_messages(self, raw_msg: str, client_id: str):
        if len(self.batches_per_client[client_id]) > 0:
            self.__process_and_send_batch(client_id)
            del self.batches_per_client[client_id]

        for routing_key in self.output_routing_keys:
            self.worker.send_message(raw_msg, routing_key)
        log.info("Sent EOF to all routing keys")

    def __process_and_send_batch(self, client_id: str):
        for result in self.__process_batch(client_id):
            result = f"{client_id}{MESSAGE_SEPARATOR}{result}"
            routing_queue = self.output_routing_keys[self.queue_to_send]
            self.worker.send_message(result, routing_queue)
            self.queue_to_send = (
                self.queue_to_send + 1) % len(self.output_routing_keys)
