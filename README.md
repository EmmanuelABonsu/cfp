# CSV Ingestion and API Service
This project implements a solution for ingesting CSV files into a database and exposing the ingested data through an API service. It includes an ETL pipeline for CSV ingestion and an API service for data retrieval.

## Features
Ingests CSV files into a relational database (SQLite in the provided example).
Exposes RESTful API endpoints to retrieve data from the database.
Logging and error handling for monitoring and debugging.
System Architecture

## Components
ETL Pipeline: Python script responsible for ingesting CSV files into the database.
API Service: Flask application that exposes RESTful API endpoints to interact with the data.
Database: Relational database management system (e.g., SQLite, PostgreSQL) for storing ingested data.
Usage

## Install Dependencies:

bash
```
poetry install
```

## Add csv file(s)
Copy your csv files into the `csv_files` folder. As an example I am using `census.csv`.

PS: The name of your csv file is used as the table name in the script. So for this example, the table name becomes census.

## Run ETL Pipeline:

bash
```
python etl_pipeline.py
```

## Start API Service:
bash ```
python api.py
```

## Access API Endpoints:

Retrieve recent data: GET /recent_data/<table_name>/<limit>

Eg: you can get the first 5 data at this endpoint `http://127.0.0.1:5000/recent_data/census/5`
