# ⚡️ UK Gridwatch Data Engineering Project 

## Objective
This project involves designing an end-to-end data pipeline using the UK Gridwatch dataset, which provides historical power consumption data. The goal is to clean, model, and visualize the data using a dashboard to present key insights, including summaries like year-on-year averages and peak demands.

You may view the live web app here: https://gridwatchuk.streamlit.app/

## Table of Content
1. [Dataset Used](#dataset-used)
2. [Technologies](#technologies)
3. [Data Pipeline Architecture](#data-pipeline-architecture)
4. [Data Modeling](#data-modelling)
5. [Pipeline Stages](#pipeline-stages)
6. [Challenges](#challenges)
7. [Limitations](#limitations)
8. [Enhancements](#Enhancements)

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
- Data Modeling: Transform the data using the star schema.
- Dashboard Design: Created computable queries to be projected onto charts. Applied simple moving average technique to smoothen lines for peak & trough analysis.
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

## Challenges
One of the main challenges I faced was understanding the Gridwatch dataset. Since this required domain knowledge, I invested time in researching energy demand, generation sources, and related metrics. This also extended to the peak and trough analysis, where I had to familiarize myself with the concept of "higher highs" and "lower lows" to effectively design the dashboard and apply line smoothing techniques.

Another challenge involved setting up DuckDB. As this was my first time working with an in-memory database, I needed to carefully study the documentation and refer to external resources to connect it successfully to my data pipeline.

## Limitations
The current pipeline is built to ingest CSV data only. Should the data size increase significantly, the performance of the Streamlit app and queries may require optimization for efficiency.

## Future Enhancements
- Adding external datasets to enrich the data model, such as integrating weather or global temperature data. This would allow for a more comprehensive analysis, showing not only energy demand and output but also how external factors impact these metrics.
- Scaling the solution to handle larger datasets by optimizing the Streamlit app performance and queries.

