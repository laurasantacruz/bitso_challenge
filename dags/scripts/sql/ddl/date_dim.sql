CREATE EXTERNAL TABLE IF NOT EXISTS bitso_de_challenge.date_dim(
    date_id int, 
    date date, 
    day int,
    month int,
    year int, 
    weekday string, 
    quarter string
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
)
STORED AS TEXTFILE
LOCATION 's3://bitso-challenge-output/dim/date/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);
