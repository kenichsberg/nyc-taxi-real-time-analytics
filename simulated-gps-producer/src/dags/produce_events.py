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
from sedona.spark import SedonaContext, KryoSerializer, SedonaKryoRegistrator
from src.schema import preprocessed_trip_data_schema


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "taxi_gps_stream"
MINUTE = 60_000
SECOND = 1_000

SIMULATED_GPS_DATA_FILE_PATH = "./src/data/generated/preprocessed.parquet"


def create_spark_session() -> SparkSession:
    try:
        spark: SparkSession = (
            SparkSession.builder
            .appName("Preprocessed data reader / transformer")
            .master("local[*]")
            .config(
                "spark.jars.packages",
                "org.apache.sedona:sedona-spark-3.5_2.12:1.7.2,"
                "org.datasyslab:geotools-wrapper:1.7.2-28.5",
            )
            .config(
                "spark.jars.repositories",
                "https://artifacts.unidata.ucar.edu/repository/unidata-all",
            )
            .config("spark.serializer", KryoSerializer.getName)
            .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
            .getOrCreate()
        )

        SedonaContext.create(spark)
        return spark

    except Exception as e:
        logging.error(f"Failed to create Spark session. {e}")
        raise(e)


# TODO Consider to split generated gps parquet into each day_of_week
def read_all_gps_data(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .schema(preprocessed_trip_data_schema)
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
    dict = row.asDict()

    event_datetime: datetime = datetime(
        current_datetime.year,
        current_datetime.month,
        current_datetime.day,
        dict.pop("hour", None),
        dict.pop("minute", None),
        dict.pop("second", None),
        math.ceil(dict.pop("microsecond", None) / 1000),
    ) 

    # timestamp `Long` in milliseconds 
    dict["timestamp"] = event_datetime.timestamp() * 1000
    
    return T.Row(**dict)


async def kafka_send_rows(
    producer: AIOKafkaProducer,
    rows: list[T.Row],
    target_second: int,
    current_datetime: datetime
):
    batch: BatchBuilder = producer.create_batch()

    for row in rows:
        #enriched_row: T.Row = add_event_datetime(row, current_datetime)
        batch.append(
            key=None,
            value=json.dumps(row.asDict()).encode("utf-8"),
            timestamp=None
        )

    delay: float = calc_delay(target_second, current_datetime.second)  
    await asyncio.sleep(delay)

    partitions: set[int] = await producer.partitions_for(TOPIC)
    partition: int = random.choice(tuple(partitions))

    return producer.send_batch(batch, TOPIC, partition=partition)


async def schedule_kafka_messages(
        producer: AIOKafkaProducer,
        rows: list[T.Row],
        target_second: int,
        current_datetime: datetime,
) -> None:
    cnt: int = len(rows)
    chunk_amount: int = 4
    rows_per_chunk: int = math.ceil(cnt / chunk_amount)
    rows_split: list[list[T.Row]] = [
        rows[i:i + rows_per_chunk] for i in range(0, cnt, rows_per_chunk)
    ]

    sent_batch = [
        kafka_send_rows(
            producer, 
            rows_split[chunk_index], 
            target_second, 
            current_datetime

        ) for chunk_index in range(0, chunk_amount) 
    ]

    [(await (await task)) for task in sent_batch]


def get_df_simulated_gps_per_minute(
    spark: SparkSession,
    timestamp: int
) -> DataFrame:

    current_datetime: datetime = datetime.fromtimestamp(timestamp)

    return (
        read_all_gps_data(spark)
        .filter(
            ((F.col("pickup_hour") < current_datetime.hour)
                | ((F.col("pickup_hour") == current_datetime.hour) 
                    & (F.col("pickup_minute") <= current_datetime.minute)))
            &
            ((F.col("dropoff_hour") > current_datetime.hour)
                | ((F.col("dropoff_hour") == current_datetime.hour)
                    & (F.col("dropoff_minute") >= current_datetime.minute)))
        )
        .withColumn(
            "simulated_event_time_second",
            F.explode(
                F.sequence(
                    F.when(
                        (F.col("pickup_hour") == current_datetime.hour) 
                            & (F.col("pickup_minute") == current_datetime.minute),
                        F.col("pickup_second")
                    )
                    .otherwise(0),
                    F.when(
                        (F.col("dropoff_hour") == current_datetime.hour) 
                            & (F.col("dropoff_minute") == current_datetime.minute),
                        F.col("dropoff_second")
                    )
                    .otherwise(60)
                )
            )
        )
        .withColumn(
            "current_epoch_seconds",
            F.make_timestamp(
                F.lit(1970),
                F.lit(1),
                F.lit(1),
                F.lit(current_datetime.hour),
                F.lit(current_datetime.minute),
                F.col("simulated_event_time_second")
            ).cast("long")
        )
        .withColumn(
            "current_trip_duration",
            F.expr("current_epoch_seconds - pickup_epoch_seconds")
        )
        .withColumn(
            "current_location",
            F.when(
                F.col("total_trip_duration") == 0,
                F.expr(
                    "ST_LineInterpolatePoint(trip_path, 1)"
                )
            )
            .otherwise(
                F.expr(
                    "ST_LineInterpolatePoint(trip_path, current_trip_duration / total_trip_duration)"
                )
            )
        )
        .select(
            "trip_id",
            F.expr("ST_Y(current_location)").alias("lat"),
            F.expr("ST_X(current_location)").alias("lon"),
            F.expr("current_epoch_seconds * 1000").alias("timestamp"),
            "simulated_event_time_second",
            "fare_amount",
            "tip_amount",
            "total_profit",
        )
        #.orderBy("second")
    )


#@dag(
#    default_args={
#        "depends_on_past": False,
#        "retries": 2,
#        "retry_delay": timedelta(seconds=1),
#    },
#    dag_id="per_minute_gps_event_producer",
#    schedule="* * * * *",
#    start_date=datetime.now(),
#    catchup=False,
#    dagrun_timeout=timedelta(minutes=1),
#)
async def run_producer_per_minute () -> None:
    spark: SparkSession = create_spark_session()

    producer: AIOKafkaProducer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
    await producer.start()

    current_ts: int = int(time.time())
    print("fetching have started")
    #rows: DataFrame = get_df_simulated_gps_per_minute(spark, current_ts)
    #print(rows.count())
    rows: DataFrame = list(get_df_simulated_gps_per_minute(spark, current_ts).toLocalIterator())
    gps_data_rows_per_second: list[list[T.Row]] = [
        [row for row in rows if row.simulated_event_time_second == second] for second in range(0, 60)
    ]
    print("fetching have completed")

    try:
        tasks = []
        for second, rows_of_second in enumerate(gps_data_rows_per_second):
            print(second, " appending")

            tasks.append(
                asyncio.create_task(
                    schedule_kafka_messages(
                        producer, 
                        rows_of_second,
                        second,
                        datetime.now()
                    )
                )
            )
            print(second, " appended")

        print("awaiting after loop")
        [(await task) for task in tasks]

    finally:
        await producer.stop()

if __name__ == "__main__":
     asyncio.run(run_producer_per_minute())
