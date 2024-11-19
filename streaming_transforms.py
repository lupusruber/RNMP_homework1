from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.time_characteristic import TimeCharacteristic


from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common import Duration

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

import json

from pathlib import Path

jar_paths = [f"file://{file.absolute()}" for file in Path("jars").glob("*.jar")]


TOPIC = "sensors"
OUTPUT_TOPIC_1 = "results1"
OUTPUT_TOPIC_2 = "results2"
BOOTSTRAP_SERVERS = "localhost:9092"

WINDOW_TIME = 10
OUT_OFF_ORDERNESS = 5


class StatsToJSONMapper(MapFunction):

    def map(self, value):

        key, start_ts, end_ts, min_element, count, average, max_element = value
        record = dict(
            key=key,
            start_ts=start_ts,
            end_ts=end_ts,
            min_element=min_element,
            count=count,
            average=average,
            max_element=max_element,
        )
        return json.dumps(record)


class WindowInfoToJSONMapper(MapFunction):

    def map(self, value):

        key, start_ts, end_ts, min_element, count, average, max_element = value
        record = dict(key=key, start_ts=start_ts, end_ts=end_ts, count=count)
        return json.dumps(record)


class WindowStatistics(ProcessWindowFunction):

    def process(self, key, context, elements):

        all_values = [element[1] for element in elements]
        count = len(all_values)
        max_element = max(all_values)
        min_element = min(all_values)
        average = sum(all_values) / count
        start_ts = context.window().start
        end_ts = context.window().end

        return [(key, start_ts, end_ts, min_element, count, average, max_element)]


class WindowInfo(ProcessWindowFunction):

    def process(self, key, context: ProcessWindowFunction.Context, elements):
        count = len(elements)
        start_ts = context.window().start
        end_ts = context.window().end

        return [(key, start_ts, end_ts, count)]


class TimestampGetter(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return int(value[2])


kafka_source = FlinkKafkaConsumer(
    topics=TOPIC,
    deserialization_schema=SimpleStringSchema(),
    properties={
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group-id": "big-data-consumer",
    },
).set_start_from_earliest()

kafka_sink_1 = FlinkKafkaProducer(
    topic=OUTPUT_TOPIC_1,
    serialization_schema=SimpleStringSchema(),
    producer_config={
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "big-data-producers",
    },
)

kafka_sink_2 = FlinkKafkaProducer(
    topic=OUTPUT_TOPIC_2,
    serialization_schema=SimpleStringSchema(),
    producer_config={
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "big-data-producers",
    },
)

WATERMARK_STRATEGY = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_seconds(OUT_OFF_ORDERNESS)
).with_timestamp_assigner(TimestampGetter())


env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
env.set_parallelism(1)
env.add_jars(*jar_paths)


data_stream = env.add_source(kafka_source)

parsed_stream = data_stream.map(
    lambda value: json.loads(value),
    output_type=Types.MAP(Types.STRING(), Types.STRING()),
).map(
    lambda record: (record["key"], float(record["value"]), int(record["timestamp"])),
    output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.LONG()]),
)


processed_stream = (
    parsed_stream.assign_timestamps_and_watermarks(WATERMARK_STRATEGY)
    .key_by(lambda tuple_: tuple_[0])
    .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME)))
    .process(
        WindowStatistics(),
        output_type=Types.TUPLE(
            # return [(key, start_ts, end_ts, min_element, count, average, max_element)]
            [
                Types.STRING(),
                Types.LONG(),
                Types.LONG(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
            ]
        ),
    )
)
stats_stream = processed_stream.map(StatsToJSONMapper(), output_type=Types.STRING())
info_stream = processed_stream.map(WindowInfoToJSONMapper(), output_type=Types.STRING())

info_stream.add_sink(kafka_sink_1)
stats_stream.add_sink(kafka_sink_2)


env.execute("Kafka Data Stream")