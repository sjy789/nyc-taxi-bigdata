# NYC Taxi Big Data Analytics and Demand Prediction

A big data analytics and demand prediction system based on
Apache Spark, Spark MLlib, Docker and Streamlit.

## Project Overview

This project processes NYC Yellow Taxi trip records
and provides data cleaning, statistical analysis,
hourly demand prediction, and interactive visualization.

## Technology Stack

- Python / PySpark
- Apache Spark 3.5.8
- Spark SQL
- Spark MLlib
- Docker and Docker Compose
- Streamlit
- Parquet / CSV / JSON

## Dataset

Source: NYC Taxi and Limousine Commission (TLC)

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Period: January to June 2024.

Valid trip records after cleaning: 19,461,117.

Raw datasets are not included in this repository.

## Features

1. Distributed taxi trip data cleaning
2. Spark SQL statistical analytics
3. Temporal and spatial demand analysis
4. Random Forest and GBT demand prediction
5. Interactive Streamlit dashboard
6. Spark performance benchmarking

## Machine Learning

Prediction task: one-hour-ahead taxi pickup demand.

Training: January to April 2024
Validation: May 2024
Testing: June 2024

Models:
- Historical baseline
- Random Forest Regression
- Gradient-Boosted Tree Regression

Best selected model: Random Forest.

Test MAE: 19.587
Test RMSE: 30.759
Test WAPE: 15.838%

## Project Structure

- apps/: Spark ETL, analytics, ML, and benchmark scripts
- stage5_app/: Streamlit dashboard
- docker-compose.yml: Spark cluster configuration
- docker-compose.dashboard.yml: Web dashboard configuration
- results/: Selected experimental results

## Running Environment

Ubuntu Linux virtual machine with Docker.
Spark standalone cluster: 1 Master and 1 Worker.

Start Spark cluster:

    sudo docker compose up -d

Start Streamlit dashboard:

    sudo docker compose -p taxi-dashboard \
      -f docker-compose.dashboard.yml up -d --build

The project requires downloading the official TLC datasets
and generating intermediate data before using the dashboard.

## Notes

Large datasets, generated Parquet files, and model weights
are not included. The processing and training scripts
are provided for reproducibility.

## Author

Independent university course project.
