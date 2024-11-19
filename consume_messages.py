from kafka import KafkaConsumer

bootstrap_servers = "localhost:9092"
topics = ["results1", "results2"]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    group_id="my_consumer_group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
)

try:
    for message in consumer:
        print(f"Topic {message.topic} message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
