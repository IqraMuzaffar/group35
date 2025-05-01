-- Move data from raw_logs to fact_user_actions
INSERT OVERWRITE TABLE fact_user_actions PARTITION (year, month, day)
SELECT 
    user_id, 
    content_id, 
    action, 
    CAST(event_timestamp AS TIMESTAMP), -- Convert to correct format
    device, 
    region, 
    session_id, 
    year(event_timestamp) AS year, 
    month(event_timestamp) AS month, 
    day(event_timestamp) AS day
FROM raw_logs;

-- Move data from raw_metadata to dim_content
INSERT OVERWRITE TABLE dim_content
SELECT * FROM raw_metadata;
