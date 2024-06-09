CREATE EXTERNAL TABLE IF NOT EXISTS bitso_de_challenge.user_dim(
    user_id string
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
)
STORED AS TEXTFILE
LOCATION 's3://bitso-challenge-output/dim/user/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);
