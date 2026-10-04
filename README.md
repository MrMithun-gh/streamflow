# StreamFlow — Real-Time Data Engineering & Monitoring Pipeline

> Work in progress. A streaming pipeline that ingests transactions through Kafka,
> processes them with PySpark Structured Streaming, validates data quality,
> stores results in PostgreSQL, and is monitored by Apache Airflow.

## Planned Architecture

Python Producer → Kafka → PySpark Structured Streaming → Data Quality Checks → PostgreSQL
Airflow orchestrates health checks and failure detection.

## Status

- [x] Project structure
- [ ] Docker environment
- [ ] Producer
- [ ] Kafka
- [ ] Spark streaming job
- [ ] PostgreSQL storage
- [ ] Data quality
- [ ] Airflow DAG
- [ ] Failure detection
