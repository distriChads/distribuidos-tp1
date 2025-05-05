
import threading
from transformers import pipeline
import logging
from .worker import Worker, WorkerConfig, MESSAGE_SEPARATOR, MESSAGE_EOF
import queue

log = logging.getLogger("machine_learning")
ML_BATCH_SIZE = 100

class MachineLearningConfig(WorkerConfig):
    pass


class MachineLearning:
    def __init__(self, config: MachineLearningConfig, output_routing_keys: list[str]):
        log.info(f"NewMachineLearning: {config.__dict__}")
        self.worker = Worker(config)
        self.sentiment_analyzer = pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased-finetuned-sst-2-english",
            device = -1,
            batch_size = ML_BATCH_SIZE,
            use_fast=True)
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
                message = body.decode("utf-8")
                client_id = message.split("|", 1)[0]
                message = message.split("|", 1)[1]
                delivery_tag = method_frame.delivery_tag
                self.messages_queue.put((client_id, message, delivery_tag))

    def __process_with_machine_learning(self, message_to_analyze: list[str]):
        results = self.sentiment_analyzer(message_to_analyze, truncation=True)
        # Aca esta guardado si es positive o negative
        return [result['label'] for result in results]
    # 5 -> budget
    # 6 -> overview
    # 7 -> revenue

    def __create_message_to_send(self, positive_or_negative: list[str], parts: list[str]):
        messages = [
            f"{positive_or_negative[i]}{MESSAGE_SEPARATOR}{parts[i][0]}{MESSAGE_SEPARATOR}{parts[i][1]}"
                for i in range(len(positive_or_negative))
            ]
 
        return "\n".join(messages)

    def __process_messages(self, overviews: list[str], parts):
        positive_or_negative = self.__process_with_machine_learning(overviews)
        return self.__create_message_to_send(positive_or_negative, parts)

    def run_worker(self):
        log.info("Starting MachineLearning worker")
        cont = 0
        while self.running:
            try:
                client_id, messages, delivery_tag = self.messages_queue.get()
                messages = messages.strip().split("\n")
                batch_for_ml = []
                parts_for_ml = []
                counter = 0
                for message in messages:
                    cont += 1
                    
                    if message == MESSAGE_EOF:

                        if len(batch_for_ml) != 0:
                            result = client_id + "|" + self.__process_messages(batch_for_ml, parts_for_ml)
                            counter = 0
                            batch_for_ml = []
                            parts_for_ml = []
                            routing_queue = self.output_routing_keys[self.queue_to_send]
                            key = routing_queue
                            self.worker.send_message(result, key)
                            self.queue_to_send = (
                                self.queue_to_send + 1) % len(self.output_routing_keys)

                        try:
                            for routing_key in self.output_routing_keys:
                                key = routing_key
                                message_to_send = client_id + "|" + MESSAGE_EOF
                                self.worker.send_message(
                                    message_to_send, key)
                            log.info(
                                f"Sent EOF to all routing keys for client {client_id}")
                        except Exception as e:
                            log.warning(f"Error sending EOF: {e}")
                        finally:
                            continue
                    if counter >= ML_BATCH_SIZE:
                        
                        result = client_id + "|" + self.__process_messages(batch_for_ml, parts_for_ml)
                        counter = 0
                        batch_for_ml = []
                        parts_for_ml = []
                        routing_queue = self.output_routing_keys[self.queue_to_send]
                        key = routing_queue
                        self.worker.send_message(result, key)
                        self.queue_to_send = (
                            self.queue_to_send + 1) % len(self.output_routing_keys)
                    parts = message.split("|")
                    counter += 1
                    batch_for_ml.append(parts[6])
                    parts_for_ml.append((parts[5], parts[7]))

            except Exception as e:
                log.error(f"Error during message processing: {e}")
                