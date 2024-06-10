from requests import Request
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
import requests
import json
import time
import hmac
import hashlib
from datetime import datetime

input_json = {
    "success": True,
    "payload": {
        "asks": [{
            "book": "btc_mxn",
            "price": "5632.24",
            "amount": "1.34491802"
        },{
            "book": "btc_mxn",
            "price": "5633.44",
            "amount": "0.4259"
        },{
            "book": "btc_mxn",
            "price": "5642.14",
            "amount": "1.21642"
        }],
        "bids": [{
            "book": "btc_mxn",
            "price": "6123.55",
            "amount": "1.12560000"
        },{
            "book": "btc_mxn",
            "price": "6121.55",
            "amount": "2.23976"
        }],
        "updated_at": "2016-04-08T17:52:31.000+00:00",
        "sequence": "27214"
    }
}


'''
Function to create the authorization header: https://docs.bitso.com/bitso-api/docs/authentication
'''
def create_api_authorization(request, api_key, api_secret):
    nonce = time.time()
    method = request.method
    url = requests.Request('GET', request.url).prepare().url
    path = requests.utils.urlparse(url).path + '?' + requests.utils.urlparse(url).query
    body = request.data if request.data else ""

    data = f"{nonce}{method}{path}{body}"
    signature = hmac.new(api_secret.encode(), data.encode(), hashlib.sha256).hexdigest()
    authorization_header = f"Bitso {api_key}:{nonce}:{signature}"

    return authorization_header


'''
Function to make API call every second during 10 minutes
'''
def get_data_from_api(book):
    api_endpoint = f"https://sandbox.bitso.com/api/v3/order_book/?book={book}"
    req = Request('GET', api_endpoint)

    # get environment variables
    api_key = Variable.get("api_key")
    api_secret = Variable.get("api_secret")

    # set the duration for making API calls (10 minutes)
    duration_seconds = 10 * 60  # 10 minutes in seconds
    end_time = time.time() + duration_seconds
    auth_header = create_api_authorization(req, api_key, api_secret)

    header = {
        "Authorization": auth_header
    }

    all_responses = {}
    today_date = datetime.today().strftime('%Y%m%d')

    while time.time() < end_time:
        try:
            response = requests.get(api_endpoint, headers=header, timeout=60)
            # check if the request was successful
            if response.status_code == 200:
                # convert the response to json
                response_json = response.json()
                print("response_json")
                print(response_json)
                all_responses.append(response_json)
            else:
                print(f"Failed to fetch data. Status code: {response.status_code}")
        except requests.RequestException as err:
            print(f"An error occurred: {err}")
        time.sleep(1)

    # upload data to s3
    s3_hook = S3Hook(aws_conn_id='aws_connection')
    bucket_name = 'bitso-challenge-1'
    file_key = f'api-response/{today_date}/output{time.time()}.json'
    
    if not all_responses:
      s3_hook.load_string(
            string_data=json.dumps(input_json),
            key=file_key,
            bucket_name=bucket_name,
            replace=True,
            encrypt=False 
        )
    else:
      s3_hook.load_string(
            string_data=json.dumps(all_responses),
            key=file_key,
            bucket_name=bucket_name,
            replace=True,
            encrypt=False 
        )
