from pyflink.datastream.window import TumblingEventTimeWindows

from pyflink.common.time import Time
from pyflink.common.typeinfo import Types

from pathlib import Path
import logging

from flink_classes import *


logger = logging.getLogger("flink_transforms_script")

jar_paths = [f"file://{file.absolute()}" for file in Path("jars").glob("*.jar")]

TOPIC = "sensors"
OUTPUT_TOPIC_1 = "results1"
OUTPUT_TOPIC_2 = "results2"
BOOTSTRAP_SERVERS = "localhost:9092"

WINDOW_TIME = 10
OUT_OF_ORDERNESS = 2


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    logger.info(f"Running Flink Application with jars: {jar_paths}")
    logger.info(f"RUnning Kafka Bootstrap servers on {BOOTSTRAP_SERVERS}")

    env = create_env(jar_paths)
    kafka_source = create_kafka_source(TOPIC, BOOTSTRAP_SERVERS)
    kafka_sink_1 = create_kafka_sink(OUTPUT_TOPIC_1, BOOTSTRAP_SERVERS)
    kafka_sink_2 = create_kafka_sink(OUTPUT_TOPIC_2, BOOTSTRAP_SERVERS)
    watermark_strategy = create_watermark_strategy(OUT_OF_ORDERNESS)

    data_stream = env.add_source(kafka_source)

    stream_with_records = data_stream.map(
        CreateRecordFromJSON(), output_type=Types.PICKLED_BYTE_ARRAY()
    )

    processed_stream = (
        stream_with_records.assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda record: record.key, key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME)))
        .process(ProcessWindowStatistics(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    stats_stream = processed_stream
    info_steam = processed_stream.map(
        WindowInfoExtractor(), output_type=Types.PICKLED_BYTE_ARRAY()
    )

    stats_stream_mapped = stats_stream.map(
        WindowStatisticsToJSONMapper(), output_type=Types.STRING()
    )
    info_stream_mapped = info_steam.map(
        WindowInfoToJSONMapper(), output_type=Types.STRING()
    )

    stats_stream_mapped.add_sink(kafka_sink_1)
    info_stream_mapped.add_sink(kafka_sink_2)

    env.execute("Kafka Data Stream")
