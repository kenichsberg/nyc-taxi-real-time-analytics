#!/bin/bash
set -e

/opt/bitnami/spark/bin/spark-submit \
  --py-files stream-transformer.zip \
  --packages org.apache.sedona:sedona-spark-3.5_2.12:1.7.2,org.datasyslab:geotools-wrapper:1.7.2-28.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --repositories https://artifacts.unidata.ucar.edu/repository/unidata-all \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator \
  main.py \
  --checkpoint /checkpoints

