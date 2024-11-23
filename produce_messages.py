import json
import time
import logging
import random

from kafka import KafkaProducer


logger = logging.getLogger(__name__)


BOOTSTRAP_SERVERS = "localhost:9092"


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    logger.info(f"RUnning Kafka Bootstrap servers on {BOOTSTRAP_SERVERS}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS, security_protocol="PLAINTEXT"
    )

    keys = ["A", "B", "C", "D"]

    while True:
        key = keys[random.randint(0, len(keys) - 1)]
        value = random.randint(0, 1000)
        record = {"key": key, "value": value, "timestamp": int(time.time() * 1000)}

        logging.info(f"Generated record: {json.dumps(record)}")
        producer.send(topic="sensors", value=json.dumps(record).encode("utf-8"))
        time.sleep(random.randint(500, 2000) / 1000.0)
