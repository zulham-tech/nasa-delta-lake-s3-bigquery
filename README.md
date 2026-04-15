# NASA Space Data Pipeline | Kafka Â· PySpark Â· Delta Lake Â· S3

> **Type:** Batch | **Stack:** NASA API â†’ Kafka â†’ PySpark â†’ Delta Lake (S3) â†’ BigQuery â†’ Airflow

## Key Metrics
- **ACID MERGE upsert:** zero duplicates on daily reruns
- **Time-travel:** VERSION AS OF 0 for historical audits
- **ZORDER** on date + event_id for faster analytical queries
- **VACUUM** auto-cleanup: old versions purged after 7 days

## Delta Lake Features
| Feature | Purpose |
|---|---|
| MERGE upsert | No duplicates on daily reruns |
| Time-travel | Historical audits |
| ZORDER | Faster queries |
| VACUUM | Auto-cleanup old versions |

## Data Sources
- NASA APOD (daily astronomy photos since 1995)
- NASA NeoWs (near-earth asteroids)
- NASA DONKI (space weather events)

## Tech Stack
Python Â· Apache Kafka Â· PySpark Â· Delta Lake Â· Amazon S3 Â· Google BigQuery Â· Airflow Â· Docker

## Author
**Ahmad Zulham Hamdan** | [LinkedIn](https://linkedin.com/in/ahmad-zulham-hamdan-665170279) | [GitHub](https://github.com/zulham-tech)
