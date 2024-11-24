from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.common import Duration

from dataclasses import dataclass
import json


@dataclass
class Record:
    key: str
    value: int
    timestamp: int


@dataclass
class WindowStatistics:
    key: str
    start_ts: int
    end_ts: int
    min_element: int
    count: int
    average: float
    max_element: int


@dataclass
class WindowInfo:
    key: str
    start_ts: int
    end_ts: int
    count: int


class CreateRecordFromJSON(MapFunction):

    def map(self, value):

        parsed_value = json.loads(value)

        return Record(**parsed_value)


class WindowStatisticsToJSONMapper(MapFunction):

    def map(self, value: WindowStatistics) -> str:

        return json.dumps(value.__dict__)


class WindowInfoToJSONMapper(MapFunction):

    def map(self, value: WindowInfo) -> str:

        return json.dumps(value.__dict__)


class WindowInfoExtractor(MapFunction):

    def map(self, value: WindowStatistics) -> WindowInfo:

        return WindowInfo(
            key=value.key,
            start_ts=value.start_ts,
            end_ts=value.end_ts,
            count=value.count,
        )


class ProcessWindowStatistics(ProcessWindowFunction):

    def process(self, key, context, elements) -> list[WindowStatistics]:

        all_values = [element.value for element in elements]

        count = len(all_values)
        max_element = max(all_values)
        min_element = min(all_values)
        average = sum(all_values) / count
        start_ts = context.window().start
        end_ts = context.window().end

        record = WindowStatistics(
            key=key,
            start_ts=start_ts,
            end_ts=end_ts,
            min_element=min_element,
            count=count,
            average=average,
            max_element=max_element,
        )

        return [record]


class TimestampGetter(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value.timestamp


def create_kafka_source(topic: str, bootstrap_servers: str) -> FlinkKafkaConsumer:

    kafka_source = FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": bootstrap_servers,
            "group-id": "big-data-consumer",
        },
    ).set_start_from_earliest()

    return kafka_source


def create_kafka_sink(topic: str, bootstrap_servers: str) -> FlinkKafkaProducer:

    kafka_sink = FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            "bootstrap.servers": bootstrap_servers,
        },
    )

    return kafka_sink


def create_watermark_strategy(out_of_orderness: int) -> WatermarkStrategy:
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(out_of_orderness)
    ).with_timestamp_assigner(TimestampGetter())

    return watermark_strategy


def create_env(jar_paths: str, parallelism: int = 1) -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(parallelism)
    env.add_jars(*jar_paths)

    return env
