from datetime import datetime, timedelta
import logging
import geopandas as gpd
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T
from sedona.sql.types import GeometryType


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC_IN = "taxi-gps"
KAFKA_TOPIC_OUT = "taxi-gps-with-zone"

GEO_DATA_FILE_PATH = "./src/data/taxi_zones.geojson"

location_data_schema: T.StructType = T.StructType([
    T.StructField("LocationID", T.IntegerType()),
    T.StructField("borough", T.StringType()),
    T.StructField("zone", T.StringType()),
    T.StructField("geometry", GeometryType()),
])

def get_df_zone(spark: SparkSession) -> DataFrame:
    pddf_location: gpd.GeoDataFrame = gpd.read_file(GEO_DATA_FILE_PATH)

    return spark.createDataFrame(
        pddf_location[["LocationID", "borough", "zone", "geometry"]],
        location_data_schema
    )


gps_stream_schema: T.StructType = T.StructType([
    T.StructField("trip_id", T.StringType()),
    T.StructField("trip_ended", T.BooleanType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("gps_timestamp", T.LongType()),
])


def add_zone_info_to_gps(spark: SparkSession) -> None:
    df_gps_stream: DataFrame = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC_IN)
        .load()
    )

    df_zone_broadcast: DataFrame = F.broadcast(get_df_zone(spark))
    #df_zone_broadcast.createOrReplaceTempView("zones")
    df_zone_broadcast.persist(StorageLevel.MEMORY_ONLY)

    df_gps_with_point: DataFrame = (
        df_gps_stream
        .withColumn(
            "parsed_values",
            F.from_json(F.col("value").cast("string"), gps_stream_schema)
        )
        .select("parsed_values.*")
        .withColumn(
            "point",
            F.expr("ST_Point(CAST(lon AS Decimal(24,20)), CAST(lat AS Decimal(24,20)))")
        )
    )

    df_joined: DataFrame = df_gps_with_point.join(
        df_zone_broadcast,
        F.expr("ST_Contains(geometry, point)")
    )

    df_kafka_messages: DataFrame = (
        df_joined
        .select(
            F.col("trip_id").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    "trip_id",
                    "trip_ended",
                    "lat",
                    "lon",
                    "gps_timestamp",
                    F.col("LocationID").alias("location_id"),
                    "borough",
                    "zone",
                )
            ).cast("string").alias("value")
        )
    )

    (df_kafka_messages.writeStream
     .format("kafka")
     .trigger(processingTime="2 seconds")
     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
     .option("topic", KAFKA_TOPIC_OUT)
     .option("checkpointLocation", f"/tmp/spark_checkpoint/{KAFKA_TOPIC_OUT}/")
     .start()
     .awaitTermination())


def main(spark: SparkSession) -> None:
    add_zone_info_to_gps(spark)
