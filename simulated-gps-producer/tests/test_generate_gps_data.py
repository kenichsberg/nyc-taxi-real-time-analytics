import random
from src.generate_gps_data import get_df_simulated_gps_per_hour, df_trip_formatted, df_trip_raw, df_exploded, df_location, spark
from pyspark.sql import functions as F, types as T

def test_generate_gps_data() -> None:
    #df_raw_data.printSchema()
    #df_raw_data.show()
    #print("raw", df_raw_data.count())
    #
    ##df_formatted.show()
    #print("formatted", df_formatted.count())
    #
    #df_exploded.show()
    #print("exploded", df_exploded.count())
    #print(df_location_data)
    #df_location_data.info()
    #print(df_location_data.size)
    #print(df_location_data.columns)


    # join
    (
        df_trip_formatted.join(
            other=df_location.withColumnRenamed("LocationID", "PULocationID"),
            on="PULocationID",
            how="left"
        )
        .withColumnRenamed("borough", "PUBorough")
        .withColumnRenamed("zone", "PUZone")
        .withColumnRenamed("geometry", "PUGeometry")
        .join(
            other=df_location.withColumnRenamed("LocationID", "DOLocationID"),
            on="DOLocationID",
            how="left"
        )
        .withColumnRenamed("borough", "DOBorough")
        .withColumnRenamed("zone", "DOZone")
        .withColumnRenamed("geometry", "DOGeometry")
        .withColumn("PULocationPoint", F.expr("ST_GeneratePoints(PUGeometry, 1)"))
        .withColumn("DOLocationPoint", F.expr("ST_GeneratePoints(DOGeometry, 1)"))
        .withColumn("trip_path", F.expr("ST_MakeLine(array(PULocationPoint, DOLocationPoint))"))
        # EXPLODE
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
            #"pickup_datetime",
            #"dropoff_datetime",
            #"duration_seconds",
            #"trip_path",
            #"event_seq_number_per_trip",
            (F.col("pickup_datetime").cast("timestamp").cast("long") + F.col("event_seq_number_per_trip"))
            .cast("timestamp").alias("timestamp"),
            F.col("current_gps"),
            F.hour("timestamp").alias("hour"),
            F.minute("timestamp").alias("minute"),
            F.second("timestamp").alias("second"),
            F.ceil(F.rand() * F.lit(1000000)).alias("microsecond"),
        )
        .show(100)

    )
    
