import asyncio
import json
import math
import random
from datetime import datetime, timedelta
import time
import logging
from airflow.decorators import dag
from aiokafka import AIOKafkaProducer
from aiokafka.producer.message_accumulator import BatchBuilder
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T
from src.schema import simulated_gps_data_schema


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "taxi_gps_stream"
MINUTE = 60_000
SECOND = 1_000

SIMULATED_GPS_DATA_FILE_PATH = "./src/data/generated/simulated_gps.parquet"


def create_spark_session() -> SparkSession:
    try:
        spark: SparkSession = (
            SparkSession.builder
            .appName("Generated data reader")
            .master("local[1]")
            .getOrCreate()
        )
        return spark

    except Exception as e:
        logging.error(f"Failed to create Spark session. {e}")
        raise(e)


# TODO Consider to split generated gps parquet into each day_of_week
def read_all_gps_data(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .schema(simulated_gps_data_schema)
        #.option("inferSchema", True)
        .option("path", SIMULATED_GPS_DATA_FILE_PATH)
        .option("header", True)
        .load()
    )


def random_delay_in_sec() -> float:
    return random.randint(1000, 4000) / 1000


def calc_delay(target_second: int, current_second: int) -> float:
    if target_second < current_second:
        return 0.0
    
    diff: int = target_second - current_second

    return diff + random_delay_in_sec()


def add_event_datetime(row: T.Row, current_datetime: datetime) -> T.Row:
    event_datetime: datetime = datetime(
        current_datetime.year,
        current_datetime.month,
        current_datetime.day,
        row.hour,
        row.minute,
        row.second,
        row.microsecond
    ) 
    dict = row.asDict()
    dict["timestamp"] = event_datetime
    
    return T.Row(**dict)


async def kafka_batch_send(
    producer: AIOKafkaProducer,
    batch: BatchBuilder,
    rows: list[T.Row],
    target_second: int,
    current_datetime: datetime
) -> None:
    for row in rows:
        enriched_row: T.Row = add_event_datetime(row, current_datetime)
        batch.append(key=None, value=json.dumps(enriched_row.asDict), timestamp=None)

    delay: float = calc_delay(target_second, current_datetime.second)  
    await asyncio.sleep(delay)

    partitions: set[int] = await producer.partitions_for(TOPIC)
    partition: int = random.choice(tuple(partitions))
    await producer.send_batch(batch, TOPIC, partition=partition)


async def schedule_kafka_messages(
        producer: AIOKafkaProducer,
        rows: list[T.Row],
        target_second: int,
        current_datetime: datetime,
) -> None:
    batch: BatchBuilder = producer.create_batch()

    cnt: int = len(rows)
    chunk_amount: int = 4
    rows_per_chunk: int = math.ceil(cnt / chunk_amount)
    rows_split: list[list[T.Row]] = [
        rows[i:i + rows_per_chunk] for i in range(0, cnt, rows_per_chunk)
    ]

    chunk_index = 0

    while chunk_index <= chunk_amount:
        asyncio.create_task(
          kafka_batch_send(
                producer, 
                batch, 
                rows_split[chunk_index], 
                target_second, 
                current_datetime
          )
        )

        batch = producer.create_batch()
        chunk_index += 1


def get_df_simulated_gps_per_minute(
    spark: SparkSession,
    timestamp: int
) -> DataFrame:

    current_datetime: datetime = datetime.fromtimestamp(timestamp)

    return (
        read_all_gps_data(spark)
        .filter(
            F.col("day_of_week") == current_datetime.isoweekday()
        )
        .filter(
            F.col("hour") == current_datetime.hour
        )
        .filter(
            F.col("minute") == current_datetime.minute
        )
        .orderBy("second", "microsecond")
    )


@dag(
    default_args={
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=1),
    },
    dag_id="per_minute_gps_event_producer",
    schedule="* * * * *",
    start_date=datetime.now(),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1),
)
async def run_producer_per_minute () -> None:
    spark: SparkSession = create_spark_session()

    producer: AIOKafkaProducer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
    await producer.start()

    current_ts: int = int(time.time())
    df_gps_data_per_minute: DataFrame = get_df_simulated_gps_per_minute(spark, current_ts)

    for second in range(1, 60):
        df_gps_data_per_second: DataFrame = (
            df_gps_data_per_minute
            .filter(F.col("second") == second)
            .drop(
                "day_of_week",
                "hour",
                "minute",
                "second",
            )
        ) 

        asyncio.create_task(
            schedule_kafka_messages(
                producer, 
                df_gps_data_per_second.collect(),
                second,
                datetime.now()
            )
        )
