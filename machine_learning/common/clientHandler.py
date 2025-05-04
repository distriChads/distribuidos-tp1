import threading
from transformers import pipeline
import logging
from .worker import Worker, WorkerConfig, MESSAGE_SEPARATOR, MESSAGE_EOF
import queue

log = logging.getLogger("machine_learning")

FIELDS_COUNT = 8
BATCH_SIZE = 10
OVERVIEW_FIELD_INDEX = 6


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
                log.info(f"Amount of batches received: {movies_received}")
                self.messages_queue.put(message)
        except Exception as e:
            log.error(f"Receive thread crashed: {e}")

    def __process_batch(self, messages: list[str]) -> list[str]:
        # Each message is the complete rabbit_msg
        results = []
        valid_messages = []

        for message in messages:
            client_id = message.split("/")[0]
            message = message.split("/")[1]
            parts = message.split(MESSAGE_SEPARATOR)
            if len(parts) < FIELDS_COUNT:
                log.warning(f"Incomplete message skipped: {parts}")
                continue
            valid_messages.append((parts, parts[OVERVIEW_FIELD_INDEX]))

        if not valid_messages:
            return results

        # Analyze sentiment in batch
        overviews = [overview for _, overview in valid_messages]
        try:
            sentiments = self.sentiment_analyzer(overviews, truncation=True)
        except Exception as e:
            log.error(f"Error in batch sentiment analysis: {e}")
            return results

        for (parts, _), sentiment in zip(valid_messages, sentiments):
            result_msg = self.__create_message_to_send(
                sentiment["label"], parts, client_id)
            results.append(result_msg)

        self.movies_processed += len(results)
        log.info(f"Processed {self.movies_processed} movies")

        return results

    def __create_message_to_send(self, sentiment: str, data: list[str], client_id):
        message = client_id + "/" + MESSAGE_SEPARATOR.join([sentiment, data[0], data[1]])
        return message

    def run_worker(self):
        log.info("Starting MachineLearning worker")
        buffer = []

        try:
            while True:
                raw_msg = self.messages_queue.get()
                raw_msg = raw_msg.strip()
                if not raw_msg:
                    continue

                if raw_msg == MESSAGE_EOF:
                    # Process any remaining messages in the buffer
                    if buffer:
                        self.__process_and_send_batch(buffer)
                        buffer.clear()

                    # Send EOF to all routing keys
                    for routing_key in self.output_routing_keys:
                        self.worker.send_message(MESSAGE_EOF, routing_key)
                    log.info("Sent EOF to all routing keys")
                    # self.__shutdown()
                    return

                messages = raw_msg.split("\n")
                buffer.extend(messages)

                if len(buffer) >= BATCH_SIZE:
                    self.__process_and_send_batch(buffer[:BATCH_SIZE])
                    buffer = buffer[BATCH_SIZE:]

        except Exception as e:
            log.error(f"Error during message processing: {e}")
            self.__shutdown()
            return e

    def __process_and_send_batch(self, buffer):
        for result in self.__process_batch(buffer):
            routing_queue = self.output_routing_keys[self.queue_to_send]
            self.worker.send_message(result, routing_queue)
            self.queue_to_send = (
                self.queue_to_send + 1) % len(self.output_routing_keys)
