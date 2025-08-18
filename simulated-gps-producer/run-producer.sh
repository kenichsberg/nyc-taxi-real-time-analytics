#!/bin/bash
set -e

start_spark() {
  echo "[ENTRYPOINT] Starting Spark..."
  /opt/bitnami/spark/bin/spark-submit \
    --conf spark.sql.files.ignoreCorruptFiles=true \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
    --py-files ./simulated-gps-producer.zip main.py \
    --checkpoint /checkpoints &
  SPARK_PID=$!
}

start_feeder() {
  echo "[ENTRYPOINT] Starting file feeder..."
  python /app/src/dags/feed_ingestion.py &
  FEEDER_PID=$!
}

start_spark
sleep 20
#start_feeder
echo "[ENTRYPOINT] Starting file feeder..."
uv run python -m src.dags.feed_ingestion
