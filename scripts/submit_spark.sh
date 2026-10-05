#!/usr/bin/env bash
# Usage: bash scripts/submit_spark.sh /app/spark/streaming_job.py
export MSYS_NO_PATHCONV=1   # stop Git Bash rewriting /app/... paths
JARS=/app/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/app/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/app/spark/jars/kafka-clients-3.4.1.jar,/app/spark/jars/commons-pool2-2.11.1.jar

docker exec -it streamflow-spark /opt/spark/bin/spark-submit \
  --master local[2] --driver-memory 1g \
  --jars "$JARS" \
  "$@"