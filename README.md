# NASA Data Pipeline — Delta Lake + S3 + BigQuery

![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=flat&logo=delta&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-FF9900?style=flat&logo=amazons3&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=googlebigquery&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)

Multi-cloud data pipeline that ingests publicly available NASA datasets (APOD, NEO asteroid tracking, FIRMS fire data, Exoplanet Archive) into a Delta Lake on S3, then replicates to Google BigQuery for cross-cloud analytics and scientific research queries.

## Architecture

```mermaid
graph TD
    A[NASA Open APIs<br/>APOD / NEO / FIRMS / Exoplanets] --> B[Python Ingestion Layer<br/>Async HTTP + Retry]
    B --> C[AWS S3<br/>Bronze Zone - Raw JSON/CSV]
    C --> D[PySpark + Delta Lake<br/>Silver Zone - Cleaned & Typed]
    D --> E[Delta Lake<br/>Gold Zone - Aggregated]
    E --> F[BigQuery External Tables<br/>via Storage Transfer]
    F --> G[BigQuery Analytics<br/>Space Science Queries]
    H[Delta Lake ACID<br/>Time Travel / Upserts] --> D
```

## Features

- Async ingestion from multiple NASA public APIs
- Delta Lake medallion architecture (Bronze → Silver → Gold)
- ACID transactions and time-travel on S3 with Delta Lake
- Upsert (MERGE) operations for asteroid tracking updates
- Cross-cloud replication from AWS S3 to Google BigQuery
- Jupyter notebooks with scientific analysis examples

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Sources | NASA Open APIs (8 datasets) |
| Raw Store | AWS S3 (Bronze) |
| Processing | PySpark + Delta Lake |
| Analytics | Google BigQuery |
| Notebooks | Jupyter + Pandas |
| Infrastructure | Docker Compose |

## Prerequisites

- Docker & Docker Compose
- AWS credentials (S3 access)
- Google Cloud credentials (BigQuery)
- NASA API Key (free at api.nasa.gov)

## Quick Start

```bash
git clone https://github.com/zulham-tech/nasa-delta-lake-s3-bigquery.git
cd nasa-delta-lake-s3-bigquery
cp .env.example .env  # add NASA_API_KEY, AWS keys, GCP credentials
docker compose up -d
python ingest/run_all.py --date today
```

## Project Structure

```
.
├── ingest/              # NASA API clients (async)
├── transforms/          # PySpark Delta Lake jobs
│   ├── bronze_to_silver/ # Cleaning & schema enforcement
│   └── silver_to_gold/   # Aggregations & enrichment
├── bigquery/            # BQ DDL & transfer configs
├── notebooks/           # Scientific analysis examples
├── docker-compose.yml
└── requirements.txt
```

## Author

**Ahmad Zulham** — [LinkedIn](https://linkedin.com/in/ahmad-zulham-665170279) | [GitHub](https://github.com/zulham-tech)
