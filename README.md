# NASA Space Data Pipeline | Delta Lake, S3, BigQuery

**Stack:** NASA API -> Kafka -> PySpark -> Delta Lake on S3 -> BigQuery -> Airflow

## Key Metrics
- ACID MERGE upsert: zero duplicates on daily reruns
- Time-travel: VERSION AS OF 0 for historical audits
- ZORDER on date + event_id for faster analytical queries
- VACUUM auto-cleanup: old versions purged after 7 days

## Delta Lake Features
| Feature | Purpose |
|---|---|
| MERGE upsert | No duplicates on reruns |
| Time-travel | Historical audits |
| ZORDER | Faster queries |
| VACUUM | Auto-cleanup old versions |

## Data Sources
- NASA APOD (daily astronomy photos since 1995)
- NASA NeoWs (near-earth asteroids)
- NASA DONKI (space weather events)

## Tech Stack
Python, Apache Kafka, PySpark, Delta Lake, Amazon S3, Google BigQuery, Airflow, Docker

## Author
Ahmad Zulham Hamdan | https://linkedin.com/in/ahmad-zulham-665170279
