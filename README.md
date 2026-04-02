# Weather Data ETL Pipeline

A streamlined ETL (Extract, Transform, Load) workflow orchestrated by **Apache Airflow**. This project processes historical weather datasets, handles data quality issues, and optimizes storage using Parquet intermediates.

## 🚀 Features
- **Extraction:** Reads raw weather data from CSV.
- **Transformation:** - Cleans column headers.
    - Removes duplicate records.
    - Implements mean-imputation for missing numeric values.
    - Fills missing text data with "unknown".
- **Storage:** Uses temporary Parquet files for efficient task-to-task data transfer.
- **Orchestration:** Fully managed by Airflow DAGs with retry logic and daily scheduling.

## 📁 Project Structure
```text
├── dags/
│   └── weather_dag.py       # Airflow DAG definition
├── datasets/
│   └── csv/
│       └── weatherHistory.csv # Input dataset
├── outputs/
│   └── cleaned_weather.csv  # Final processed data
└── README.md
