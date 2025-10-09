# BTC Price ETL Pipeline

## 🔰 Overview

This project is a fully working ETL pipeline built with Apache Airflow. It extracts Bitcoin order book snapshots from multiple exchanges, calculates the best price for a given order volume, and loads the results into a PostgreSQL database.

The pipeline demonstrates dynamic task mapping, XCom usage for passing data between tasks, and a clean separation of extract, transform, and load stages.

Status

✅ Project complete. Currently organizing files and documentation for GitHub presentation.

Key Features

💠Automated data extraction from multiple cryptocurrency exchanges  
💠Calculation of best price and detailed quotes  
💠PostgreSQL integration with proper relational schema  
💠Airflow DAG with dynamic task mapping and scheduled runs








