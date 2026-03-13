# NYC Taxi Data Pipeline

A simple **data engineering pipeline** that downloads NYC taxi trip
data, processes it using **Python and pandas**, and loads it into a
**PostgreSQL database running in Docker**.

This project demonstrates core data engineering concepts including:

- Data ingestion
- Chunk processing for large datasets
- Containerized databases
- Reproducible environments
- Pipeline scripting

---

# Tech Stack

- Python
- Pandas
- SQLAlchemy
- PostgreSQL
- Docker
- Docker Compose
- uv (Python dependency management)

---

# Project Structure

pipeline/

├── Dockerfile\
├── docker-compose.yaml\
├── ingest_data.py\
├── pipeline.py\
├── main.py\
├── notebook.ipynb\
├── pyproject.toml\
├── uv.lock\
└── README.md

---

# Setup Instructions

## 1. Start the database

Run PostgreSQL and pgAdmin using Docker Compose.

docker compose up -d

This will start:

PostgreSQL → port 5432\
pgAdmin → port 8085

---

## 2. Access pgAdmin

Open your browser:

http://localhost:8085

Login:

email: admin@admin.com\
password: root

Database connection settings:

host: pgdatabase\
port: 5432\
user: root\
password: root\
database: ny_taxi

---

## 3. Install Python dependencies

This project uses **uv** for dependency management.

uv sync

---

## 4. Run the ingestion pipeline

uv run python ingest_data.py

The script will:

1.  Download NYC taxi trip data
2.  Read the dataset in chunks
3.  Create a PostgreSQL table
4.  Insert records into the database

---

# Data Processing Strategy

The dataset is processed in chunks of **100,000 rows** to avoid loading
large files entirely into memory.

---

# Learning Goals

This project demonstrates:

- Containerized database setup
- Chunk-based data ingestion
- Pandas → SQL pipelines
- Environment reproducibility
- Docker-based workflows
