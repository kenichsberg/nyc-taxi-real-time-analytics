from datetime import datetime, timedelta
import logging
import geopandas as gpd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T
from pyspark.storagelevel import StorageLevel
from sedona.spark import SedonaContext, KryoSerializer, SedonaKryoRegistrator
from src.schema import taxi_trip_data_schema, location_data_schema


TRIP_DATA_FILE_PATH = "./src/data/yellow_tripdata_2025-05.parquet"
GEO_DATA_FILE_PATH = "./src/data/taxi_zones.geojson"
SIMULATED_GPS_DATA_PATH = "./src/data/generated/simulated_gps"
TEMP_DATA_PATH_BEFORE_EXPLODING = "./src/data/generated/intermediate/before_exploding"
TEMP_DATA_PATH_AFTER_EXPLODING = "./src/data/generated/intermediate/after_exploding"

def create_spark_session() -> SparkSession:
    try:
        spark: SparkSession = (
            SparkSession.builder
            .appName("Taxi trip data preprocessor")
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


def get_df_location(spark: SparkSession) -> DataFrame:
    pddf_location: gpd.GeoDataFrame = gpd.read_file(GEO_DATA_FILE_PATH)

    return spark.createDataFrame(
        pddf_location[["LocationID", "borough", "zone", "geometry"]],
        location_data_schema
    )

def get_df_trip(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .schema(taxi_trip_data_schema)
        .option("path", TRIP_DATA_FILE_PATH)
        .option("header", True)
        .load()
    )


def join_df_trip_with_location(df_trip: DataFrame, df_location: DataFrame) -> DataFrame:
    return (
        df_trip
        .select(
            F.col("tpep_pickup_datetime").alias("pickup_datetime"),
            F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long") -  F.col("tpep_pickup_datetime").cast("timestamp").cast("long")).alias("duration"),
            F.col("PULocationID"),
            F.col("DOLocationID"),
            "fare_amount",
            "tip_amount",
            F.expr("fare_amount + tip_amount").alias("total_profit"),
        )
        .join(
            other=df_location.withColumnRenamed("LocationID", "PULocationID"),
            on="PULocationID",
            how="inner"
        )
        .withColumnRenamed("borough", "PUBorough")
        .withColumnRenamed("zone", "PUZone")
        .withColumnRenamed("geometry", "PUGeometry")
        .join(
            other=df_location.withColumnRenamed("LocationID", "DOLocationID"),
            on="DOLocationID",
            how="inner"
        )
        .withColumnRenamed("borough", "DOBorough")
        .withColumnRenamed("zone", "DOZone")
        .withColumnRenamed("geometry", "DOGeometry")
    )


def save_before_exploding(df_trip_with_location: DataFrame) -> None:
    """To prevent from exhausting memory by exploding (see explode_and_save),
    Saves all data with partitions, so they can be fetched with smaller chunks."""
    df_intermediate: DataFrame = (
        df_trip_with_location
        ####
        # NOTE Here adjusting the total data volume
        .withColumn("day_of_week", F.dayofweek("pickup_datetime"))
        .filter(F.col("day_of_week") == 5)
        ####
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("PUGeometry").isNotNull())
        .filter(F.col("DOGeometry").isNotNull())

        .withColumn("trip_id", F.expr("uuid()"))
        .withColumn("PULocationPoint", F.expr("ST_GeneratePoints(PUGeometry, 1)"))
        .withColumn("DOLocationPoint", F.expr("ST_GeneratePoints(DOGeometry, 1)"))
        .withColumn("trip_path", F.expr("ST_MakeLine(array(PULocationPoint, DOLocationPoint))"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_minute", F.minute("pickup_datetime"))
        .withColumn(
            # NOTE In case duration is longer than 1000 sec, exploding event_seq_number per sec can exhaust memory.
            # Here create a limit to avoid possibilities of OutOfMemoryError
            "total_event_amount",
            F.when(
                F.col("duration") >= 1000,
                1000
            )
            .otherwise(F.col("duration"))
        )
    )

    print("[BEFORE Exploding]: writing data")
    (df_intermediate.write
     .partitionBy("pickup_hour", "pickup_minute")
     .mode("overwrite")
     .parquet(TEMP_DATA_PATH_BEFORE_EXPLODING))


def explode_and_save(spark: SparkSession, hour: int, minute: int) -> None:
    """Expands each row to event_seq_number. Each new row will be treated
    as a record of GPS info of taxi location at every second 
    (or less frequently if the trip is long)."""
    print(f"[AFTER Exploding]: generating data of {hour:02d}:{minute:02d}")
    df_intermediate: DataFrame = (
        spark.read
        #.schema(taxi_trip_data_schema)
        .option("inferSchema", True)
        .option("path", TEMP_DATA_PATH_BEFORE_EXPLODING + f"/pickup_hour={hour}/pickup_minute={minute}")
        .option("header", True)
        .load()
    )

    df_intermediate: DataFrame = (
        df_intermediate
        .withColumn(
            "event_seq_number",
            F.explode(
                F.sequence(
                    F.lit(0),
                    F.when(
                        F.col("duration") <= 0,
                        0
                    )
                    .otherwise(F.col("total_event_amount"))
                )
            )
        )
        .withColumn(
            "timestamp",
            (F.col("pickup_datetime").cast("timestamp").cast("long") + (F.col("duration") * F.col("event_seq_number") / F.col("total_event_amount")).cast("long")).cast("timestamp")
        )
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("minute", F.minute("timestamp"))
        .withColumn("second", F.second("timestamp"))
        .withColumn("microsecond", F.ceil(F.rand() * F.lit(1000)))
    )

    (df_intermediate.write
     .partitionBy("minute")
     .mode("append")
     .parquet(TEMP_DATA_PATH_AFTER_EXPLODING))



def calculate_current_location_and_save(spark: SparkSession, minute: int) -> None:
    """For each exploded row, adds the current geo-location (lat/lon) calculated by
    Linestring and event_seq_number."""
    print(f"[GPS DATA]: generating data of {(minute + 1):2d}/60")
    df_intermediate: DataFrame = (
        spark.read
        #.schema(taxi_trip_data_schema)
        .option("inferSchema", True)
        # NOTE To avoid OutOfMemoryError, load and transform data in a small chunk
        .option("path", TEMP_DATA_PATH_AFTER_EXPLODING + f"/minute={minute}")
        .option("header", True)
        .load()
    )

    df_gps_data: DataFrame = (
        df_intermediate
        .withColumn(
            "current_location",
            F.when(
                F.col("duration") == 0,
                F.expr(
                    "ST_LineInterpolatePoint(trip_path, 1)"
                )
            )
            .otherwise(
                F.expr(
                    "ST_LineInterpolatePoint(trip_path, event_seq_number / total_event_amount)"
                )
            )
        )
        .select(
            "trip_id",
            F.expr("ST_Y(current_location)").alias("lat"),
            F.expr("ST_X(current_location)").alias("lon"),
            "timestamp",
            "hour",
            F.minute("timestamp").alias("minute"),
            "second",
            "microsecond",
            "fare_amount",
            "tip_amount",
            "total_profit",
        )
        .orderBy("minute", "second", "microsecond")
    )

    (df_gps_data.write
     .partitionBy("minute", "second")
     .mode("append")
     .parquet(SIMULATED_GPS_DATA_PATH))



def join_and_save(spark: SparkSession) -> None:
    df_location: DataFrame = get_df_location(spark)

    df_trip_raw: DataFrame = get_df_trip(spark)

    df_trip_with_location: DataFrame = join_df_trip_with_location(df_trip_raw, df_location)

    save_before_exploding(df_trip_with_location)



def main() -> None:
    spark: SparkSession = create_spark_session()

    try:
        join_and_save(spark)

        hour_minute_pair: list[tuple[int, int]] = [(h, m) for h in range(0, 24) for m in range(0, 60)]
        for hour, minute in hour_minute_pair:
            explode_and_save(spark, hour, minute)

        for minute in range(0, 60):
            calculate_current_location_and_save(spark, minute)

    except Exception as e:
        logging.error(f"Spark processing failed. {e}")
        raise(e)


if __name__ == "__main__":
    main()
