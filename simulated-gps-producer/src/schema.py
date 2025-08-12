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

simulated_gps_data_schema: T.StructType = T.StructType([
    T.StructField("trip_id", T.StringType()),
    T.StructField("trip_ended", T.BooleanType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("hour", T.IntegerType()),
    T.StructField("microsecond", T.LongType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("total_profit", T.DoubleType()),
    T.StructField("pickup_location_id", T.IntegerType()),
    T.StructField("pickup_borough", T.StringType()),
    T.StructField("pickup_zone", T.StringType()),
    T.StructField("dropoff_location_id", T.IntegerType()),
    T.StructField("dropoff_borough", T.StringType()),
    T.StructField("dropoff_zone", T.StringType()),
])
