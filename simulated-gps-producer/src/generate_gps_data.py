from datetime import datetime, timedelta
import logging
import geopandas as gpd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T
from src.schema import taxi_trip_data_schema, location_data_schema
from sedona.spark import SedonaContext, KryoSerializer, SedonaKryoRegistrator


TRIP_DATA_FILE_PATH = "./src/data/yellow_tripdata_2025-05.parquet"
GEO_DATA_FILE_PATH = "./src/data/taxi_zones.geojson"
SIMULATED_GPS_DATA_FILE_PATH = "./src/data/generated/simulated_gps.parquet"

def create_spark_session() -> SparkSession:
    try:
        spark: SparkSession = (
            SparkSession.builder
            .appName("Simulated gps data generator")
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
            F.dayofweek("tpep_pickup_datetime").alias("day_of_week"),
            (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long") -  F.col("tpep_pickup_datetime").cast("timestamp").cast("long")).alias("duration_seconds"),
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
        .withColumn(
            "event_seq_number_per_trip",
            F.explode(
                F.sequence(
                    F.lit(0),
                    F.when(
                        F.col("duration_seconds") <= 0,
                        0
                    )
                    .otherwise(
                        F.col("duration_seconds") - F.lit(1)
                    )
                )
            )
        )
        .withColumn(
            "current_gps",
            F.when(
                F.col("duration_seconds") == 0,
                F.expr(
                    "ST_LineInterpolatePoint(trip_path, 1)"
                )
            )
            .otherwise(
                F.expr(
                    "ST_LineInterpolatePoint(trip_path, event_seq_number_per_trip / duration_seconds)"
                )
            )
        )
        .select(
            (F.col("pickup_datetime").cast("timestamp").cast("long") + F.col("event_seq_number_per_trip"))
            .cast("timestamp").alias("timestamp"),
            F.expr("ST_Y(current_gps)").alias("lat"),
            F.expr("ST_X(current_gps)").alias("lon"),
            F.col("day_of_week"),
            F.hour("timestamp").alias("hour"),
            F.minute("timestamp").alias("minute"),
            F.second("timestamp").alias("second"),
            F.ceil(F.rand() * F.lit(1000000)).alias("microsecond"),
            F.col("fare_amount"),
            F.col("tip_amount"),
            F.col("total_profit"),
        )
        ###
        #.filter(
        #    F.col("day_of_week") == 6
        #)
        #.filter(
        #    (F.col("hour") == 21) | (F.col("hour") == 22) 
        #)
        ###
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
