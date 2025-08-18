from datetime import datetime, timedelta
import os
from pathlib import Path
import logging
from airflow.decorators import dag
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T
from src.schema import simulated_gps_data_schema

KAFKA_BOOTSTRAP_SERVER = "kafka:29092"
KAFKA_TOPIC_GPS = "taxi-gps"
KAFKA_TOPIC_PAYMENT = "taxi-payment"

dir = os.path.dirname(__file__)
PARTITIONED_DIR = dir / Path("../data/generated/simulated_gps")
INGESTION_DIR = dir / Path("../data/ingestion/")


def write_to_kafka_per_second(spark: SparkSession) -> None:
    df: DataFrame = (
        spark.readStream
        .schema(simulated_gps_data_schema)
        .option("header", True)
        .parquet(INGESTION_DIR.absolute().as_posix())
    )

    # Fan-out: gps | payment

    df_taxi_gps: DataFrame = (
        df
        .withColumn(
            "gps_timestamp",
            (F.current_timestamp().cast("double") * 1000).cast("long")
        )
        .select(
            F.col("trip_id").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    "trip_id",
                    "trip_ended",
                    "lat",
                    "lon",
                    "gps_timestamp",
                )
            ).cast("string").alias("value")
        )
    )

    (df_taxi_gps.writeStream
     .format("kafka")
     .trigger(processingTime="1 second")
     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
     .option("topic", KAFKA_TOPIC_GPS)
     .option("checkpointLocation", f"/tmp/spark_checkpoint/{KAFKA_TOPIC_GPS}/")
     .start())


    df_taxi_payment: DataFrame = (
        df
        .filter(F.col("trip_ended"))
        .withColumn(
            "payment_timestamp",
            (F.current_timestamp().cast("double") * 1000).cast("long")
        )
        .select(
            F.col("trip_id").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    "trip_id",
                    "payment_timestamp",
                    "fare_amount",
                    "tip_amount",
                    "total_profit",
                    "pickup_location_id",
                    "pickup_borough",
                    "pickup_zone",
                    "dropoff_location_id",
                    "dropoff_borough",
                    "dropoff_zone",
                )
            ).cast("string").alias("value")
        )
    )

    (df_taxi_payment.writeStream
     .format("kafka")
     .trigger(processingTime="1 second")
     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
     .option("topic", KAFKA_TOPIC_PAYMENT)
     .option("checkpointLocation", f"/tmp/spark_checkpoint/{KAFKA_TOPIC_PAYMENT}")
     .start())

    spark.streams.awaitAnyTermination()



def main(spark: SparkSession) -> None:
    write_to_kafka_per_second(spark)
