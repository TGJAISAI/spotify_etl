import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
from airflow.utils.dates import days_ago
from Transfrom import trans
from Extract import getdata
from Load import load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2023,1,29),
    'email': ['tgjaisai@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    'spotify_final_dag',
    default_args=default_args,
    description='Spotify ETL process 1-min',
    schedule_interval=dt.timedelta(minutes=50),
)



with dag:    
    task1= PythonOperator(
        task_id='spotify_etl_final',
        python_callable = getdata ,
        dag = dag)
    

    task2 = PythonOperator(
        task_id='spotify_etl_final',
        python_callable=trans,
        dag=dag,
    )

    task3 = PythonOperator(
        task_id='spotify_etl_final',
        python_callable=load,
        dag=dag,
    )


    task1 >> task2 >> task3