
from transformers import pipeline
import logging
from .worker import Worker, WorkerConfig, MESSAGE_SEPARATOR, MESSAGE_EOF

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

    def __process_with_machine_learning(self, message_to_analyze: str):
        result = self.sentiment_analyzer(message_to_analyze, truncation=True)
        # Aca esta guardado si es positive o negative
        return result[0]['label']
    # 5 -> budget
    # 6 -> overview
    # 7 -> revenue

    def __create_message_to_send(self, positive_or_negative: str, parts: str):
        return positive_or_negative + MESSAGE_SEPARATOR + parts[5] + MESSAGE_SEPARATOR + parts[7]

    def __receive_message(self, rabbit_msg: str):
        parts = rabbit_msg.split(MESSAGE_SEPARATOR)
        positive_or_negative = self.__process_with_machine_learning(parts[6])
        return self.__create_message_to_send(positive_or_negative, parts)

    def run_worker(self):
        log.info("Starting MachineLearning worker")
        try:
            self.worker.init_sender()
            self.worker.init_receiver()
        except Exception as e:
            log.error(f"Error initializing worker: {e}")
            return e

        try:
            for method_frame, properties, body in self.worker.received_messages():
                message = body.decode("utf-8")
                log.debug(f"Received message: {message}")
                if message == MESSAGE_EOF:
                    log.info("Received EOF")
                    try:
                        for routing_key in self.output_routing_keys:
                            self.worker.send_message(MESSAGE_EOF, routing_key)
                            log.debug(f"Sent EOF to {routing_key}")
                    except Exception as e:
                        log.warning(f"Error sending EOF: {e}")
                    break
                lines = message.strip().split("\n")
                for line in lines:
                    message = self.__receive_message(line)
                    routing_queue = self.output_routing_keys[self.queue_to_send]
                    self.worker.send_message(message, routing_queue)
                    self.queue_to_send = (
                        self.queue_to_send + 1) % len(self.output_routing_keys)
        except Exception as e:
            log.error(f"Error during message processing: {e}")
            return e

        log.info("MachineLearning worker finished")
        return None
