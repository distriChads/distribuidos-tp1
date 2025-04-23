from common.clientHandler import MachineLearning, MachineLearningConfig
from common.worker import ExchangeSpec



def main():
    # Hardcodeo de configuraciones
    log_level = "INFO"  # Opcional si querés ajustar el logging
    input_exchange_spec = ExchangeSpec(
        name="query_exchange",
        routing_keys=["movies.input"],
        queue_name="machine_learning_queue"
    )
    output_exchange_spec = ExchangeSpec(
        name="query_exchange",
        routing_keys=["machine.learning.output"],
        queue_name="machine_learning_queue"
    )
    message_broker = "amqp://guest:guest@localhost:5672/"

    # Creación del worker
    filter_config = MachineLearningConfig(
        input_exchange=input_exchange_spec,
        second_input_exchange="",
        output_exchange=output_exchange_spec,
        message_broker=message_broker
    )

    worker = MachineLearning(filter_config)

    try:
        worker.run_worker()
    except Exception as e:
        print("Triste")
    finally:
        worker.worker.close_worker()

if __name__ == "__main__":
    main()