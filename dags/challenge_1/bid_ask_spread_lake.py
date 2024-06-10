'''
DAG that reads the current day records and calculates the spread
'''
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import json


def read_jsons_from_s3(bucket_name, prefix):
    
    s3_hook = S3Hook(aws_conn_id='aws_connection')

    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    
    json_data = {
        "asks": [],
        "bids": []
    }

    for key in keys:
        content = s3_hook.read_key(key, bucket_name)
        json_content = json.loads(content)
        
        if json_content.get('success'):
            payload = json_content.get('payload', {})
            json_data['asks'].extend(payload.get('asks', []))
            json_data['bids'].extend(payload.get('bids', []))
    return json_data


def calculate_spread(bucket_name, prefix):
    data = read_jsons_from_s3(bucket_name, prefix)
    # get the lowest ask
    ask_prices_list = []

    for elem in data["asks"]:
        ask_prices_list.append(elem["price"])

    lowest_ask_price = min(ask_prices_list)
    # get the highest bid
    bid_prices_list = []

    for elem in data["bids"]:
      bid_prices_list.append(elem["price"])

    highest_bid_price = max(bid_prices_list)

    spread = abs((float(lowest_ask_price) - float(highest_bid_price))*100 / float(lowest_ask_price))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

today_date = datetime.today().strftime('%Y%m%d')
BUCKET_NAME = "bitso-challenge-1"
PREFIX = f"api-response/{today_date}"
SCHEDULE_INTERVAL = "0 0 * * *"

with DAG(
    'bid_ask_spread', 
    default_args=default_args,
    chedule_interval=SCHEDULE_INTERVAL
) as dag:
    # dummies
    init = DummyOperator(task_id='init')
    end = DummyOperator(task_id='end')

    calculate_spread = PythonOperator(
        task_id='aggregate_jsons_from_s3',
        provide_context=True,
        python_callable=calculate_spread,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            'prefix': PREFIX
        }
    )

    init >> calculate_spread >> end
