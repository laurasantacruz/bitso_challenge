CREATE EXTERNAL TABLE IF NOT EXISTS bitso_de_challenge.transaction_fact(
    id int, 
    event_timestamp string,
    user_id string, 
    amount double, 
    currency string, 
    tx_status string,
    transaction_type string,
    date_id int
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
)
STORED AS TEXTFILE
LOCATION 's3://bitso-challenge-output/fact/transaction/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);
