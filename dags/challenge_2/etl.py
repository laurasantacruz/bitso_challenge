'''
ETL of deposit, withdrawal, event and user. Create fact and dim tables.
Created: June 8th 2024
'''
from airflow import DAG   
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.etl_functions import *
import pendulum

# schedule to run every day
SCHEDULE_INTERVAL = "0 0 * * *"
INPUT_BUCKET = 'bitso-lz'
OUTPUT_BUCKET = 'bitso-challenge-output'

default_args = {
    'owner': 'Laura',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 6, tzinfo=pendulum.timezone('America/Mexico_City')),
    'email_on_failure': True,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 3,
    'retry_delay': timedelta(minutes=4),
    "catchup": False,
    "trigger_rule": 'all_success'
}

with DAG(   
    dag_id="fact_dim_etl",
    default_args=default_args,
    description='pipeline',
    schedule_interval=SCHEDULE_INTERVAL
) as dag:
    # dummies
    init = DummyOperator(task_id='init')
    dim_dummy = DummyOperator(task_id ='dim_checkpoint')
    end = DummyOperator(task_id='end')

    build_user_dim = PythonOperator(
        task_id='user_dim',
        python_callable=build_user_dim,
        op_kwargs={'input_bucket': INPUT_BUCKET,
                   'output_bucket': OUTPUT_BUCKET,
                   'aws_conn_id':'aws_connection'}
    )

    build_time_dim = PythonOperator(
        task_id='time_dim',
        python_callable=build_time_dim,
        op_kwargs={'input_bucket': INPUT_BUCKET,
                   'output_bucket': OUTPUT_BUCKET,
                   'aws_conn_id':'aws_connection'}
    )

    build_logins_fact = PythonOperator(
        task_id='logins_fact',
        python_callable=build_logins_fact,
        op_kwargs={'input_bucket': INPUT_BUCKET,
                   'output_bucket': OUTPUT_BUCKET,
                   'aws_conn_id':'aws_connection'}
    )

    build_transactions_fact = PythonOperator(
        task_id='transactions_fact',
        python_callable=build_transactions_fact,
        op_kwargs={'input_bucket': INPUT_BUCKET,
                   'output_bucket': OUTPUT_BUCKET,
                   'aws_conn_id':'aws_connection'}
    )

    init >> [build_user_dim, build_time_dim] >> dim_dummy
    dim_dummy >> [build_logins_fact, build_transactions_fact] >> end


