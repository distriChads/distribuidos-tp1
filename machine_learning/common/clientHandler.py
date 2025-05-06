import threading
from transformers import pipeline
import logging
from .worker import Worker, WorkerConfig, MESSAGE_SEPARATOR, MESSAGE_EOF
import queue

log = logging.getLogger("machine_learning")

FIELDS_COUNT = 8
BATCH_SIZE = 100
OVERVIEW_FIELD_INDEX = 6
MAX_WORDS = 30
BUDGET_INDEX = 5
REVENUE_INDEX = 7


class MachineLearningConfig(WorkerConfig):
    pass


class MachineLearning:
    def __init__(self, config: MachineLearningConfig, output_routing_keys: list[str]):
        log.info(f"NewMachineLearning: {config.__dict__}")
        self.worker = Worker(config)
        self.movies_processed = 0
        self.sentiment_analyzer = pipeline(
            'sentiment-analysis',
            model='distilbert-base-uncased-finetuned-sst-2-english',
            batch_size=BATCH_SIZE,
        )
        self.output_routing_keys = output_routing_keys
        self.queue_to_send = 0

        try:
            self.worker.init_sender()
            self.worker.init_receiver()
        except Exception as e:
            log.error(f"Error initializing worker: {e}")
            return e

        self._running = True
        self.messages_queue = queue.Queue()
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

    def __process_batch(self, messages: list[str]) -> list[str]:
        # Each message is the complete rabbit_msg
        results = []
        valid_messages = []
        revenue_0_in_batch = 0

        for message in messages:
            parts = message.split(MESSAGE_SEPARATOR)
            if len(parts) < FIELDS_COUNT:
                log.warning(f"Incomplete message skipped: {parts}")
                continue
            if parts[BUDGET_INDEX] == "0" or parts[REVENUE_INDEX] == "0":
                self.movies_processed += 1
                revenue_0_in_batch += 1
                continue
            overviews_truncated = self.__truncate_to_words(
                parts[OVERVIEW_FIELD_INDEX])
            valid_messages.append((parts, overviews_truncated))

        log.info(
            f"Amount of movies with revenue or budget 0 in batch: {revenue_0_in_batch}")

        if not valid_messages:
            return results

        # Analyze sentiment in batch
        overviews = [overview for _, overview in valid_messages]
        try:
            sentiments = self.sentiment_analyzer(overviews)
        except Exception as e:
            log.error(f"Error in batch sentiment analysis: {e}")
            return results

        for (parts, _), sentiment in zip(valid_messages, sentiments):
            result_msg = self.__create_message_to_send(
                sentiment["label"], parts)
            results.append(result_msg)

        self.movies_processed += len(results)
        log.info(f"Processed {self.movies_processed} movies")

        return results

    def __truncate_to_words(self, text):
        words = text.split()
        return ' '.join(words[:MAX_WORDS])

    def __create_message_to_send(self, sentiment: str, parts: list[str]):
        result = MESSAGE_SEPARATOR.join([sentiment, parts[5], parts[7]])
        return result
        # return positive_or_negative + MESSAGE_SEPARATOR + parts[5] + MESSAGE_SEPARATOR + parts[7]

    def run_worker(self):
        log.info("Starting MachineLearning worker")
        buffer = []

        try:
            while True:
                raw_msg = self.messages_queue.get()
                raw_msg = raw_msg.strip()
                if not raw_msg:
                    continue

                client_id, message = raw_msg.split(MESSAGE_SEPARATOR, 1)
                if message == MESSAGE_EOF:
                    # Process any remaining messages in the buffer
                    if buffer:
                        self.__process_and_send_batch(client_id, buffer)
                        buffer.clear()

                    # Send EOF to all routing keys
                    for routing_key in self.output_routing_keys:
                        self.worker.send_message(raw_msg, routing_key)
                    log.info("Sent EOF to all routing keys")

                messages = raw_msg.split("\n")
                buffer.extend(messages)

                if len(buffer) >= BATCH_SIZE:
                    self.__process_and_send_batch(
                        client_id, buffer[:BATCH_SIZE])
                    buffer = buffer[BATCH_SIZE:]

        except Exception as e:
            log.error(f"Error during message processing: {e}")
            self.__shutdown()
            return e

    def __process_and_send_batch(self, client_id, buffer):
        for result in self.__process_batch(buffer):
            result = f"{client_id}{MESSAGE_SEPARATOR}{result}"
            routing_queue = self.output_routing_keys[self.queue_to_send]
            self.worker.send_message(result, routing_queue)
            self.queue_to_send = (
                self.queue_to_send + 1) % len(self.output_routing_keys)
