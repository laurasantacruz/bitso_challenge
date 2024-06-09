from dotenv import load_dotenv
import os
import boto3
import pandas as pd
import csv

load_dotenv()
# get environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

'''
Function to read the raw csv from s3
'''
def get_data(bucket, key):
  s3_client = boto3.client(
      "s3",
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
  )

  response = s3_client.get_object(Bucket=bucket, Key=key)
  status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

  if status == 200:
      print(f"Successful S3 get_object response. Key:{key}. Status - {status}")
      df = pd.read_csv(response.get("Body"))
  else:
      print(f"Unsuccessful S3 get_object response. Status - {status}")
  return df


'''
Function to write df as csv in s3
'''
def write_data(bucket_name, file_path, df):
   s3_path = f's3://{bucket_name}/{file_path}'
   try:
      df.to_csv(s3_path, index=False, storage_options={
         'key': aws_access_key_id,
         'secret': aws_secret_access_key})
   except Exception as err:
      print(f"An error ocurred while writing: {err}")


'''
Function to build user dim, this dimension doesn't need any transformation
'''
def build_user_dim(input_bucket, output_bucket):
   output_key = "dim/user_dim.csv"
   key = "20240607/user_id_sample_data.csv"
   user_df = get_data(input_bucket, key)

   write_data(output_bucket, key, user_df)


'''
Function to buil the time dime, the start date is the first day where we have records
'''
def build_time_dim(output_bucket):
    ouput_key = "dim/date_dim.csv"
   # date range to cover
    start_date = '2020-01-01'
    end_date = '2024-12-31'
    date_range = pd.date_range(start=start_date, end=end_date)

    # Create a DataFrame with the date attributes
    dim_date_df = pd.DataFrame({
        'date_id': date_range.strftime('%Y%m%d').astype(int), 
        'date': date_range,
        'day': date_range.day,
        'month': date_range.month,
        'year': date_range.year,
        'weekday': date_range.strftime('%A'),
        'quarter': date_range.quarter,
    })

    write_data(output_bucket, ouput_key, dim_date_df)


'''
Function to buld transaction fact
A single table for both withdrawals and deposit was made by merging both tables
'''
def build_transactions_fact(input_bucket, output_bucket):
   output_key = "fact/transactions_fact.csv"
   deposit_key = "20240607/deposit_sample_data.csv"
   withdrawal_key = "20240607/withdrawals_sample_data.csv"
   deposit_df = get_data(input_bucket, deposit_key)
   withdrawal_df = get_data(input_bucket, withdrawal_key)
   # create column to identify the type of transaction
   deposit_df["transaction_type"] = 'deposit'
   withdrawal_df["transaction_type"] = 'withdrawal'
   # select required columns
   withdrawal_df_final = withdrawal_df[['id', 'event_timestamp','user_id', 'amount', 'currency', 'tx_status', 'transaction_type']]
   # final merged df
   transaction_fact_df = pd.concat([deposit_df, withdrawal_df_final], ignore_index=True, sort=False)

   # add date_id
   transaction_fact_df['event_timestamp'] = pd.to_datetime(transaction_fact_df['event_timestamp'], errors='coerce')
   transaction_fact_df['date_id'] = transaction_fact_df['event_timestamp'].dt.strftime('%Y%m%d')

   write_data(output_bucket, output_key, transaction_fact_df)


'''
Function to build logins fact.
Only transformation is to add date_id
'''
def build_logins_fact(input_bucket, output_bucket):
   output_key = "fact/logins_fact.csv"
   input_key = "20240607/event_sample_data.csv"
   event_df = get_data(input_bucket, input_key)
   login_events_df = event_df[event_df["event_name"] == 'login']
   event_fact_df = login_events_df[['id', 'event_timestamp', 'user_id']]

   # add date_id
   event_fact_df['event_timestamp'] = pd.to_datetime(event_fact_df['event_timestamp'], errors='coerce')
   event_fact_df['date_id'] = event_fact_df['event_timestamp'].dt.strftime('%Y%m%d')

   write_data(output_bucket, output_key, event_fact_df)



