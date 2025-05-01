-- Create raw logs table
CREATE EXTERNAL TABLE raw_logs (
    user_id INT,
    content_id INT,
    action STRING,
    event_timestamp STRING,  -- Renamed from 'timestamp'
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/raw/logs';

-- Create raw metadata table
CREATE EXTERNAL TABLE raw_metadata (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/raw/metadata';

-- Create fact table
CREATE TABLE fact_user_actions (
    user_id INT,
    content_id INT,
    action STRING,
    event_timestamp TIMESTAMP,
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET;

-- Create dimension table
CREATE TABLE dim_content (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
STORED AS PARQUET;
