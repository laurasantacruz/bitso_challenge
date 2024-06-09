from requests import Request
from dotenv import load_dotenv
import requests
import json
import time
import pendulum
import hmac
import hashlib
import os

# load environment variables
load_dotenv()

'''
Function to create the authorization header: https://docs.bitso.com/bitso-api/docs/authentication
'''
def create_api_authorization(request, api_key, api_secret):
    nonce = time.time()
    method = request.method
    url = requests.Request('GET', request.url).prepare().url
    path = requests.utils.urlparse(url).path + '?' + requests.utils.urlparse(url).query
    body = request.body if request.body else ""

    data = f"{nonce}{method}{path}{body}"
    signature = hmac.new(api_secret.encode(), data.encode(), hashlib.sha256).hexdigest()
    authorization_header = f"Bitso {api_key}:{nonce}:{signature}"
    return authorization_header


'''
Function to make API call every second during 10 minutes
'''
def get_data_from_api(book=''):
    api_endpoint = f"https://sandbox.bitso.com/api/v3/order_book?book={book}"
    req = Request('GET', api_endpoint)

    # get environment variables
    api_key = os.getenv('API_KEY')
    api_secret = os.getenv('API_SECRET')

    # set the duration for making API calls (10 minutes)
    duration_seconds = 10 * 60  # 10 minutes in seconds
    end_time = time.time() + duration_seconds
    auth_header = create_api_authorization(req, api_key, api_secret)
    header = {
        "Authorization": auth_header
    }

    while time.time() < end_time:
        try:
            response = requests.get(api_endpoint, headers=header, timeout=60)
            # check if the request was successful
            if response.status_code == 200:
                # convert the response to json
                response_json = response.json()
                print(response_json)
            else:
                print(f"Failed to fetch data. Status code: {response.status_code}")
        except requests.RequestException as err:
            print(f"An error occurred: {err}")
        time.sleep(1)
        
        # write the json (could be to s3 if possible)
