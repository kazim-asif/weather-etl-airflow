import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import sys

from airflow.datasets import Dataset
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

DAG_FOLDER = Path(__file__).parent.resolve()

FILE_NAME = 'weatherHistory'
FILE_PATH = DAG_FOLDER.parent / 'datasets' / 'csv' / f'{FILE_NAME}.csv'
CSV_OUTPUT = DAG_FOLDER.parent / 'outputs' / f'cleaned_{FILE_NAME}.csv'

def extractData(**kwargs):
    print(f"Reading data from {FILE_PATH}")

    df = pd.read_csv(FILE_PATH)
    df.columns = df.columns.str.strip().str.replace('"', '')
    
    tmp_path = f'/tmp/{FILE_NAME}.parquet'
    df.to_parquet(tmp_path)
    return tmp_path


def transformData(**kwargs):
    print(f"CLeaning and transforming data.")

    # Pull the file path from the previous task
    ti = kwargs['ti']
    tmp_path = ti.xcom_pull(task_ids='extract_task')
    raw_df = pd.read_parquet(tmp_path)

    # Remove duplicates
    cleaned_df = raw_df.drop_duplicates().copy()

    # Handle numeric missing values
    numeric_cols = raw_df.select_dtypes(include=['number']).columns
    cleaned_df[numeric_cols] = raw_df[numeric_cols].fillna(raw_df[numeric_cols].mean())

    # Handle text missing values
    text_cols = raw_df.select_dtypes(include=['object']).columns
    cleaned_df[text_cols] = raw_df[text_cols].fillna('unknown')

    clean_tmp_path = f'/tmp/clean_{FILE_NAME}.parquet'
    cleaned_df.to_parquet(clean_tmp_path)
    return clean_tmp_path

def loadData(**kwargs):

    ti = kwargs['ti']
    temp_path = ti.xcom_pull(task_ids='transform_task')
    df = pd.read_parquet(temp_path)

    print(f"Loading data to file {CSV_OUTPUT}")
    
    # Save to CSV
    df.to_csv(CSV_OUTPUT, index=False)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_dag',
    default_args=default_dag_args,
    description='A simple ETL pipeline',
    schedule='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extractData
    )

    transform_task = PythonOperator(
        task_id = 'transform_task',
        python_callable = transformData
    )

    load_task = PythonOperator(
        task_id = 'load_task',
        python_callable = loadData
    )
    
    # Define Dependency (The Flow)
    extract_task >> transform_task >> load_task
