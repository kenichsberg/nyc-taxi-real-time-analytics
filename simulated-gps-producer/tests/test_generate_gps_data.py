from pyspark.sql import SparkSession, DataFrame
from src.generate_gps_data import create_spark_session, get_df_location, get_df_trip, join_df_trip_with_location, generate_simulated_gps_data

def test_generate_gps_data() -> None:
    spark: SparkSession = create_spark_session()

    df_location: DataFrame = get_df_location(spark)

    df_trip_raw: DataFrame = get_df_trip(spark)

    df_trip_with_location: DataFrame = join_df_trip_with_location(df_trip_raw, df_location)

    df_simulated_gps: DataFrame = generate_simulated_gps_data(df_trip_with_location)

    df_simulated_gps.show(100)
    
