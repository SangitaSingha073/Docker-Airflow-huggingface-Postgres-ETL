# Hugging Face ETL Pipeline using Apache Airflow (Docker)

This project demonstrates a complete **ETL pipeline using Apache Airflow** running in a **Dockerized environment**.  
The pipeline extracts model metadata from the Hugging Face Hub API, transforms the data, and loads it into a PostgreSQL database.

pgAdmin is used to visualize and explore the stored data.

## Project Architecture

Hugging Face API
|
▼
Apache Airflow
(Extract → Transform → Load)
|
▼
PostgreSQL Database
|
▼
pgAdmin

## ETL Pipeline

The pipeline performs three stages:

### 1. Extract

Fetches model metadata from Hugging Face Hub using the Hugging Face API.

Fields extracted:

- Model ID
- Author
- Pipeline Tag
- Tags
- Last Modified Date

### 2. Transform

Data cleaning and preprocessing:

- Removes duplicate models
- Handles missing values
- Formats timestamps
- Standardizes fields

### 3. Load

The processed data is stored in PostgreSQL in the table:

```
ai_models
```

Schema:

```
model_id VARCHAR PRIMARY KEY
author VARCHAR
pipeline_tag VARCHAR
tags TEXT[]
last_modified TIMESTAMP
```

## Installation

### 1 Clone repository

```
git clone https://github.com/yourusername/airflow-huggingface-etl-pipeline.git
cd airflow-huggingface-etl-pipeline
```

### 2 Start Docker containers

```
docker compose up -d
```

### 3 Open Airflow UI

```
http://localhost:8080
```

Default credentials:

```
username: airflow
password: airflow
```

---

## Access pgAdmin

```
http://localhost:5051
```

Login:

```
email: admin@example.com
password: admin
```
