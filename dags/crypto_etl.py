from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

def extract_data():
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false"
    response = requests.get(url)
    data = response.json()
    return data

def transform_data(**context):
    data = context['ti'].xcom_pull(task_ids='extract_task')
    df = pd.DataFrame(data)
    df = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume']]
    return df.to_dict('records')

def load_data(**context):
    data = context['ti'].xcom_pull(task_ids='transform_task')
    df = pd.DataFrame(data)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    df.to_sql('crypto_prices', engine, if_exists='replace', index=False)

with DAG(
    dag_id='crypto_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS crypto_prices;
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                current_price FLOAT,
                market_cap BIGINT,
                total_volume BIGINT
            );
        """
    )

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data
    )

    create_table >> extract_task >> transform_task >> load_task