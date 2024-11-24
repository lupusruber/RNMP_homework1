from kafka import KafkaConsumer
import logging

BOOTSTRAP_SERVERS = "localhost:9092"
topics = ["results1", "results2"]


logger = logging.getLogger(__name__)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    logger.info(f"Running Kafka Application on {BOOTSTRAP_SERVERS}")

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="my_consumer_group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    try:
        for message in consumer:
            logger.info(
                f" Topic {message.topic} message: {message.value.decode('utf-8')}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
