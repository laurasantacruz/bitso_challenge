'''
DAG to get monitor the bid-ask spread from order books 
Created: June 7th 2024
'''
from airflow import DAG   
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pendulum
from scripts.api_call import *

# schedule to run every 10 minutes
SCHEDULE_INTERVAL = "*/10 * * * *"

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
    dag_id="bid_ask_spread",
    default_args=default_args,
    description='pipeline',
    schedule_interval=SCHEDULE_INTERVAL
) as dag:
    # dummies
    init = DummyOperator(task_id='init')
    end = DummyOperator(task_id='end')

    get_data_from_api_btc_mxn = PythonOperator(
        task_id='get_data_btc_mxn',
        python_callable=get_data_from_api,
        op_kwargs={'book': 'btc_mxn'}
    )

    init >> get_data_from_api_btc_mxn >> end

    

