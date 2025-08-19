#!/bin/bash
set -euo pipefail

# Start Spark in background
/opt/bitnami/spark/bin/spark-submit \
  --conf spark.sql.files.ignoreCorruptFiles=true \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --py-files ./simulated-gps-producer.zip main.py \
  --checkpoint /checkpoints &
SPARK_PID=$!

# healthcheck
for i in {1..30}; do
  if nc -z localhost 4040; then
    echo "[ENTRYPOINT] Spark is up"
    break
  fi
  echo "[ENTRYPOINT] Waiting for Spark..."
  sleep 2
done

# Start feeder in background
uv run python -m src.dags.feed_ingestion &
FEEDER_PID=$!

# Wait for both
wait -n $SPARK_PID $FEEDER_PID
exit $?
