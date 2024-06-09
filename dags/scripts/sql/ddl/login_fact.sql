CREATE EXTERNAL TABLE IF NOT EXISTS bitso_de_challenge.login_fact(
    id int, 
    event_timestamp string,
    user_id string, 
    date_id int
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
)
STORED AS TEXTFILE
LOCATION 's3://bitso-challenge-output/fact/event/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);
