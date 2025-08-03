from pyspark.sql import types as T
from sedona.sql.types import GeometryType

taxi_trip_data_schema: T.StructType = T.StructType([
    T.StructField("VendorID", T.IntegerType()),
    T.StructField("tpep_pickup_datetime", T.TimestampNTZType(), False),
    T.StructField("tpep_dropoff_datetime", T.TimestampNTZType(), False),
    T.StructField("passenger_count", T.LongType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("RatecodeID", T.LongType()),
    T.StructField("store_and_fwd_flag", T.StringType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("payment_type", T.LongType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("extra", T.DoubleType()),
    T.StructField("mta_tax", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("tolls_amount", T.DoubleType()),
    T.StructField("improvement_surcharge", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("congestion_surcharge", T.DoubleType()),
    T.StructField("airport_fee", T.DoubleType()),
    T.StructField("cbd_congestion_fee", T.DoubleType()),
])

location_data_schema: T.StructType = T.StructType([
    T.StructField("LocationID", T.IntegerType()),
    T.StructField("borough", T.StringType()),
    T.StructField("zone", T.StringType()),
    T.StructField("geometry", GeometryType()),
])

preprocessed_trip_data_schema: T.StructType = T.StructType([
    T.StructField("trip_id", T.StringType()),
    T.StructField("trip_path", GeometryType()),
    T.StructField("total_trip_duration", T.LongType()),
    T.StructField("pickup_hour", T.IntegerType()),
    T.StructField("pickup_minute", T.IntegerType()),
    T.StructField("pickup_second", T.IntegerType()),
    T.StructField("pickup_microsecond", T.LongType()),
    T.StructField("pickup_epoch_seconds", T.LongType()),
    T.StructField("dropoff_hour", T.IntegerType()),
    T.StructField("dropoff_minute", T.IntegerType()),
    T.StructField("dropoff_second", T.IntegerType()),
    T.StructField("dropoff_microsecond", T.LongType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("total_profit", T.DoubleType()),
])
