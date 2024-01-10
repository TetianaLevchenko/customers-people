import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import logging

import json
import pandas
from datetime import datetime
import numpy as np
from pathlib import Path

def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]
    processed_user = pandas.json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'] })
    processed_user.to_csv('/opt/data/processed_user.csv', index=None, header=False)

def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')

    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/opt/data/processed_user.csv' # previously '/tmp/processed_user.csv'
    )

def _read_countries():
    hook = PostgresHook(postgres_conn_id='postgres')

    with open('/opt/data/countries.csv') as file_obj:
        reader_obj = csv.reader(file_obj)
        next(reader_obj, None) # skip headers

        rows = []

        for row in reader_obj:
            rows.append([
                row[0].strip(),
                row[1].strip(),
                int(row[2]),
            ])

        print (rows)

        hook.insert_rows(table = 'countries', rows = rows,
                         target_fields = ['name', 'region', 'population'],
                         commit_every = 1000, replace = True, replace_index="name")



def _save_countries_to_excel(ti):

    hook = PostgresHook(postgres_conn_id="postgres")
    df2 = hook.get_pandas_df(sql="select * from countries")

    print("we got some sql results back!")

    print(df2)

    # Create multiple lists
    technologies =  ['Spark','Pandas','Java','Python', 'PHP']
    fee = [1,20000,15000,15000,18000]
    duration = ['5o Days','35 Days',np.nan,'30 Days', '30 Days']
    discount = [2000,1000,800,500,800]
    columns=['Courses','Fee','Duration','Discount']

    # Create DataFrame from multiple lists
    df = pandas.DataFrame(list(zip(technologies,fee,duration,discount)), columns=columns)

    countries_file = '/opt/data/Courses.xlsx'

    my_file = Path(countries_file)
    if not my_file.is_file():
        df.to_excel(countries_file, sheet_name='Test')

    with pandas.ExcelWriter(countries_file, mode = 'a', if_sheet_exists= 'replace') as writer:
        df.to_excel(writer, sheet_name='Test')
        df2.to_excel(writer, sheet_name='Schedule')

with DAG('user_processing', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    create_user_table = PostgresOperator(
        task_id='create_user_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )

    create_country_table = PostgresOperator(
        task_id='create_country_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS countries (
                name varchar(100) NOT NULL primary key,
                region varchar(100),
                population bigint
            );
        '''
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user
    )

    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )

    read_countries = PythonOperator(
        task_id="read_countries",
        python_callable=_read_countries
    )

    save_countries_to_excel = PythonOperator(
        task_id="save_countries_to_excel",
        python_callable=_save_countries_to_excel
    )

    create_user_table >> is_api_available >> extract_user >> process_user >> store_user
    create_country_table >> read_countries >> save_countries_to_excel
