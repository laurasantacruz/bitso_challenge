from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
import pandas as pd


'''
Function to read the raw csv from s3
'''
def get_data(bucket, key, aws_conn_id):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
    # Read the CSV content from S3
    file_obj = s3_hook.get_key(key, bucket).get()
    df = pd.read_csv(file_obj['Body'])
    return df


'''
Function to write df as csv in s3
'''
def write_data(bucket_name, file_path, df, aws_conn_id):
   s3_hook = S3Hook(aws_conn_id=aws_conn_id)

   # Save the DataFrame content as a CSV file to S3
   csv_buffer = StringIO()
   df.to_csv(csv_buffer, index=False)
   s3_hook.load_string(csv_buffer.getvalue(), file_path, bucket_name, replace=True)


'''
Function to build user dim, this dimension doesn't need any transformation
'''
def build_user_dim(input_bucket, output_bucket, aws_conn_id):
   output_key = "dim/user/user_dim.csv"
   key = "20240607/user_id_sample_data.csv"
   user_df = get_data(input_bucket, key, aws_conn_id)

   write_data(output_bucket, output_key, user_df, aws_conn_id)


'''
Function to buil the time dime, the start date is the first day where we have records
'''
def build_time_dim(output_bucket, aws_conn_id):
    ouput_key = "dim/date/date_dim.csv"
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

    write_data(output_bucket, ouput_key, dim_date_df, aws_conn_id)


'''
Function to buld transaction fact
A single table for both withdrawals and deposit was made by merging both tables
'''
def build_transactions_fact(input_bucket, output_bucket, aws_conn_id):
   output_key = "fact/transaction/transactions_fact.csv"
   deposit_key = "20240607/deposit_sample_data.csv"
   withdrawal_key = "20240607/withdrawals_sample_data.csv"
   deposit_df = get_data(input_bucket, deposit_key, aws_conn_id)
   withdrawal_df = get_data(input_bucket, withdrawal_key, aws_conn_id)
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

   # rename columns
   transaction_fact_df.rename(columns = {'id':'transaction_id',
                                         'event_timestamp':'login_timestamp', 
                                         'amount':'transaction_amount',
                                         'tx_status':'transaction_status'}, 
                                         inplace = True)


   write_data(output_bucket, output_key, transaction_fact_df, aws_conn_id)


'''
Function to build logins fact.
Only transformation is to add date_id
'''
def build_logins_fact(input_bucket, output_bucket, aws_conn_id):
   output_key = "fact/login/logins_fact.csv"
   input_key = "20240607/event_sample_data.csv"
   event_df = get_data(input_bucket, input_key, aws_conn_id)
   login_events_df = event_df[event_df["event_name"] == 'login']
   event_fact_df = login_events_df[['id', 'event_timestamp', 'user_id']]

   # add date_id
   event_fact_df['event_timestamp'] = pd.to_datetime(event_fact_df['event_timestamp'], errors='coerce')
   event_fact_df['date_id'] = event_fact_df['event_timestamp'].dt.strftime('%Y%m%d')

   # rename columns
   event_fact_df.rename(columns = {'id':'login_id', 
                                   'event_timestamp':'login_timestamp'},
                                     inplace = True)

   write_data(output_bucket, output_key, event_fact_df, aws_conn_id)



