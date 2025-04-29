
import threading
from transformers import pipeline
import logging
from .worker import Worker, WorkerConfig, MESSAGE_SEPARATOR, MESSAGE_EOF
import queue

log = logging.getLogger("machine_learning")


class MachineLearningConfig(WorkerConfig):
    pass


class MachineLearning:
    def __init__(self, config: MachineLearningConfig, output_routing_keys: list[str]):
        log.info(f"NewMachineLearning: {config.__dict__}")
        self.worker = Worker(config)
        self.sentiment_analyzer = pipeline(
            'sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
        self.output_routing_keys = output_routing_keys
        self.queue_to_send = 0
        self.running = True

        try:
            self.worker.init_sender()
            self.worker.init_receiver()
        except Exception as e:
            log.error(f"Error initializing worker: {e}")
            return e

        self.messages_queue = queue.Queue()
        self.thread = threading.Thread(
            target=self.__receive_messages, daemon=True)
        self.thread.start()

    def __receive_messages(self):
        log.info("Starting message receiver thread")
        while self.running:
            for method_frame, _properties, body in self.worker.received_messages():
                client_id = method_frame.routing_key.split(".")[0]
                delivery_tag = method_frame.delivery_tag
                message = body.decode("utf-8")
                self.messages_queue.put((client_id, message, delivery_tag))

    def __process_with_machine_learning(self, message_to_analyze: str):
        result = self.sentiment_analyzer(message_to_analyze, truncation=True)
        # Aca esta guardado si es positive o negative
        return result[0]['label']
    # 5 -> budget
    # 6 -> overview
    # 7 -> revenue

    def __create_message_to_send(self, positive_or_negative: str, parts: list[str]):
        return positive_or_negative + MESSAGE_SEPARATOR + parts[5] + MESSAGE_SEPARATOR + parts[7]

    def __process_message(self, rabbit_msg: str):
        parts = rabbit_msg.split(MESSAGE_SEPARATOR)
        positive_or_negative = self.__process_with_machine_learning(parts[6])
        return self.__create_message_to_send(positive_or_negative, parts)

    def run_worker(self):
        log.info("Starting MachineLearning worker")
        cont = 0
        while self.running:
            try:
                client_id, messages, delivery_tag = self.messages_queue.get()
                messages = messages.strip().split("\n")
                for message in messages:
                    cont += 1
                    if message == MESSAGE_EOF:
                        try:
                            for routing_key in self.output_routing_keys:
                                key = client_id + "." + routing_key
                                self.worker.send_message(
                                    MESSAGE_EOF, key)
                            log.info(
                                f"Sent EOF to all routing keys for client {client_id}")
                        except Exception as e:
                            log.warning(f"Error sending EOF: {e}")
                        finally:
                            self.worker.send_ack(delivery_tag)
                            continue

                    result = self.__process_message(message)
                    routing_queue = self.output_routing_keys[self.queue_to_send]
                    key = client_id + "." + routing_queue
                    self.worker.send_message(result, key)
                    self.queue_to_send = (
                        self.queue_to_send + 1) % len(self.output_routing_keys)
                
                self.worker.send_ack(delivery_tag)
            except Exception as e:
                log.error(f"Error during message processing: {e}")
                