from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta

default_args ={
    'owner':'tetiana',
    
    
}

with DAG (
    dag_id='test_dag_tutorial',
    default_args = default_args,
    start_date=datetime(2023,1,1),
    schedule_interval='@once',
    catchup=False
) as dag:
    task1= PostgresOperator (
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql = """
        CREATE TABLE IF NOT EXISTS people (
        Index SERIAL PRIMARY KEY,
        User_ID VARCHAR NOT NULL,
        Last_Name VARCHAR NOT NULL,
        Sex VARCHAR NOT NULL,
        Email VARCHAR NOT NULL,
        Phone VARCHAR NOT NULL,
        Date_of_birth DATE NOT NULL,
        Job_Title  VARCHAR NOT NULL
        

        )
        """
    )
    task1
