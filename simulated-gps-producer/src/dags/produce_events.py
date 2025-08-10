import asyncio
import json
import math
import random
from datetime import datetime, timedelta
import os
from pathlib import Path
import logging
from airflow.decorators import dag
from aiokafka import AIOKafkaProducer
from aiokafka.producer.message_accumulator import BatchBuilder
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T
from src.schema import simulated_gps_data_schema


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "taxi_gps_stream"

#TODO move to a common file
dir = os.path.dirname(__file__)
SIMULATED_GPS_DATA_BASE_PATH = dir + "/../data/generated/simulated_gps"
SPARK_READ_DIR_PATH = dir + "/../data/spark_stream_read/"

PARTITIONED_DIR = dir / Path("../data/generated/simulated_gps")
INGESTION_DIR = dir / Path("../data/ingestion/")

def write_to_kafka_per_second(spark: SparkSession) -> None:
    now: datetime = datetime.now()

    df: DataFrame = (
        spark.readStream
        .schema(simulated_gps_data_schema)
        .option("header", True)
        .parquet(INGESTION_DIR.absolute().as_posix())
        .select(
            F.col("trip_id").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    "trip_id",
                    "lat",
                    "lon",
                    F.make_timestamp(
                        F.lit(now.year),
                        F.lit(now.month),
                        F.lit(now.day),
                        F.hour("timestamp"),
                        F.minute("timestamp"),
                        F.second("timestamp") + F.expr("microsecond / 1000")
                    ).cast("long").alias("gps_timestamp"),
                    "fare_amount",
                    "tip_amount",
                    "total_profit"
                )
            ).cast("string").alias("value")
        )
    )

    (df.writeStream
     .format("kafka")
     .trigger(processingTime="1 second")
     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
     .option("topic", KAFKA_TOPIC)
     .option("checkpointLocation", "/tmp/spark_checkpoint/")
     .start()
     .awaitTermination()
     )

def main(spark: SparkSession) -> None:
    write_to_kafka_per_second(spark)


#if __name__ == "__main__":
#    main()
