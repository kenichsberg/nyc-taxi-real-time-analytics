from pyspark.sql import SparkSession
import logging
from src.dags.produce_events import main 

def create_spark_session() -> SparkSession:
    try:
        spark: SparkSession = (
            SparkSession.builder
            .appName("Preprocessed data reader / transformer")
            .master("local[*]")
            .getOrCreate()
        )

        spark.sparkContext.addPyFile("./simulated-gps-producer.zip")

        return spark

    except Exception as e:
        logging.error(f"Failed to create Spark session. {e}")
        raise(e)

spark = create_spark_session()

if __name__ == "__main__":
    main(spark)
