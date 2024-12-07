import json
import time
import logging
import random
import sys


from kafka import KafkaProducer


logger = logging.getLogger("kafka_producer_script")


BOOTSTRAP_SERVERS = "localhost:9092"


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    logger.info(f"RUnning Kafka Bootstrap servers on {BOOTSTRAP_SERVERS}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS, security_protocol="PLAINTEXT"
    )

    keys = ["A", "B", "C", "D"]

    while True:
        
        key = random.choice(keys)
        value = random.randint(0, 1000)
        record = {"key": key, "value": value, "timestamp": int(time.time() * 1000)}

        try:
           
            value = json.dumps(record)
            logger.info(f" Generated record: {value}")
            producer.send(topic="sensors", value=value.encode("utf-8"))
            time.sleep(random.randint(500, 2000) / 1000.0)

        except KeyboardInterrupt:
            logger.info("Exiting gracefully...")
            producer.close()
            sys.exit(0)
