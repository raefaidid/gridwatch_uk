# ⚡️ UK Gridwatch Data Engineering Project 

## Objective
This project involves designing an end-to-end data pipeline using the UK Gridwatch dataset, which provides historical power consumption data. The goal is to clean, model, and visualize the data using a dashboard to present key insights, including summaries like year-on-year averages and peak demands.

## Table of Content
1. [Dataset Used](##dataset-used)
2. [Technologies](##technologies)
3. [Data Pipeline Architecture](##data-pipeline-architecture)
4. [Data Modeling](##data-modelling)
5. [Pipeline Stages](##pipeline-stages)

## Dataset Used
Source: [UK Gridwatch Dataset](https://www.gridwatch.templar.co.uk/)
Fields: Contains 23 columns including timestamp, recorded demand, and contributions from various energy sources.

## Technologies
- Programming Language: Python
- Database: DuckDB (in-memory)
- Visualization: Streamlit
- Other Tools: draw.io for data modelling
- Hosting: Streamlit Hosting

## Data Pipeline Architecture
The architecture follows a layered approach:

- Download CSV: Collect data from the Gridwatch website.
- Ingest into In-Memory DB: Load the data into DuckDB for fast querying.
- Storage orgnization: Following the Bronze-Silver-Gold model.
- Data Modeling: Transform the data using the star schmea.
- Web App Development: Build and deploy a dashboard using Streamlit.
- Hosting: Host the Streamlit app online.


## Data Modeling
The data is modeled using Kimball’s star schema methodology, resulting in:

1. Fact Table: Contains the 1 primary key and foreign keys from dimension tables (e.g., energy_id, datetime_id).
2. Dimension Tables: Datetime Dimension, Energy Dimension

## Pipeline Stages
- Step 0: Set up in-memory DB using DuckDB.
- Step 1: Ingest the dataset into DuckDB.
- Step 2: Store raw data in the bronze schema.
- Step 3: Clean and transform data in Python.
- Step 4: Load transformed data into the silver schema for analysis.
- Step 5: Develop web app and dashboards using Streamlit.
- Step 6: Host the dashboard and deploy it on Streamlit Hosting.
