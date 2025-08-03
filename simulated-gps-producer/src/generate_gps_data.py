from datetime import datetime, timedelta
import logging
import geopandas as gpd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T
from sedona.spark import SedonaContext, KryoSerializer, SedonaKryoRegistrator
from src.schema import taxi_trip_data_schema, location_data_schema


TRIP_DATA_FILE_PATH = "./src/data/yellow_tripdata_2025-05.parquet"
GEO_DATA_FILE_PATH = "./src/data/taxi_zones.geojson"
SIMULATED_GPS_DATA_FILE_PATH = "./src/data/generated/preprocessed.parquet"

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
            (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long") -  F.col("tpep_pickup_datetime").cast("timestamp").cast("long")).alias("total_trip_duration"),
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


def generate_simulated_gps_data(df_trip_with_location: DataFrame) -> DataFrame:
    return (
        df_trip_with_location
        .withColumn("PULocationPoint", F.expr("ST_GeneratePoints(PUGeometry, 1)"))
        .withColumn("DOLocationPoint", F.expr("ST_GeneratePoints(DOGeometry, 1)"))
        .withColumn("trip_path", F.expr("ST_MakeLine(array(PULocationPoint, DOLocationPoint))"))
        .select(
            F.expr("uuid()").alias("trip_id"),
            "trip_path",
            "total_trip_duration",
            F.hour("pickup_datetime").alias("pickup_hour"),
            F.minute("pickup_datetime").alias("pickup_minute"),
            F.second("pickup_datetime").alias("pickup_second"),
            F.ceil(F.rand() * F.lit(1000)).alias("pickup_microsecond"),
            F.make_timestamp(F.lit(1970), F.lit(1), F.lit(1), "pickup_hour", "pickup_minute", "pickup_second").cast("long").alias("pickup_epoch_seconds"),
            F.hour("dropoff_datetime").alias("dropoff_hour"),
            F.minute("dropoff_datetime").alias("dropoff_minute"),
            F.second("dropoff_datetime").alias("dropoff_second"),
            F.ceil(F.rand() * F.lit(1000)).alias("dropoff_microsecond"),
            "fare_amount",
            "tip_amount",
            "total_profit",
        )
    )


def main() -> None:
    spark: SparkSession = create_spark_session()

    try:
       df_location: DataFrame = get_df_location(spark)

       df_trip_raw: DataFrame = get_df_trip(spark)

       df_trip_with_location: DataFrame = join_df_trip_with_location(df_trip_raw, df_location)

       df_simulated_gps: DataFrame = generate_simulated_gps_data(df_trip_with_location)
       
       # Save generated data
       (
           df_simulated_gps.write
           .format("parquet")
           .mode("overwrite")
           .save(SIMULATED_GPS_DATA_FILE_PATH)
       )

    except Exception as e:
        logging.error(f"Spark processing failed. {e}")
        raise(e)


if __name__ == "__main__":
    main()
