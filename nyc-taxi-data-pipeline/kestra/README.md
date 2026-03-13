# Kestra Data Engineering Workflows

This repository contains example workflows built with **Kestra** as part of learning data engineering pipelines.

The workflows demonstrate:

- Basic workflow execution
- Running Python scripts in Docker
- Building a simple ETL pipeline

---

# 1. Hello World Workflow

This workflow demonstrates the basic structure of a Kestra pipeline.

## Features

- Print input and output messages
- Use variables
- Demonstrate task execution order
- Pause the workflow for **15 seconds**

## Workflow Steps

![Architecture](https://drive.google.com/file/d/1kvE3QNJFIgLuKVtBf3kEH4uYFNknS7zk/view?usp=drive_link)

This workflow helps understand the fundamental concepts of Kestra such as:

- Inputs
- Variables
- Tasks
- Outputs
- Scheduling

---

# 2. Python Workflow

This workflow demonstrates how Kestra can execute Python scripts inside a Docker container.

## Pipeline Overview

The workflow performs the following steps:

1. Run Python inside a Docker container
2. Install required Python packages
3. Call the Docker Hub API
4. Retrieve the number of downloads for the **Kestra Docker image**
5. Store the result as a workflow output
6. Track execution metrics

## Execution Flow

![Architecture](https://drive.google.com/file/d/13jfiYJ0OUZpSDm5uqftMNc16NRcGijCh/view?usp=drive_link)
This example shows how Kestra can orchestrate containerized scripts using Docker.

---

# 3. ETL Data Pipeline

This workflow demonstrates a simple **ETL (Extract, Transform, Load) pipeline** using Kestra.

## Pipeline Steps

### Extract

Download product data from an external API.

### Transform

Use Python to filter and process the JSON data.

### Query

Use **DuckDB** to run SQL queries on the transformed data.

## Pipeline Architecture

![Architecture](https://drive.google.com/file/d/1-wUgtK2oeO_DvoaGhjPdN5BI5eXGnPgL/view?usp=drive_link)
This pipeline demonstrates how Kestra can orchestrate:

- API data ingestion
- Python-based transformations
- SQL analytics with DuckDB

---

# Technologies Used

- Kestra
- Docker
- Python
- DuckDB
- REST APIs

---

# Purpose of This Repository

This project was created to learn and practice:

- Workflow orchestration
- Containerized task execution
- Data pipeline development
- ETL pipeline design
