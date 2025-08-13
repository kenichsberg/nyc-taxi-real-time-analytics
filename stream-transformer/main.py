from pyspark.sql import SparkSession
import logging
from sedona.spark import SedonaContext, KryoSerializer, SedonaKryoRegistrator
from src.dags.enrich_gps_stream import main 

def create_spark_session() -> SparkSession:
    try:
        spark: SparkSession = (
            SparkSession.builder
            .appName("Kafka Stream Transformer")
            .master("local[2]")
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


spark: SparkSession = create_spark_session()

if __name__ == "__main__":
    main(spark)
