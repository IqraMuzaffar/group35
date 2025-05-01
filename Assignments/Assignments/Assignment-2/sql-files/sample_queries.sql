-- Monthly Active Users by Region
SELECT region, COUNT(DISTINCT user_id) AS active_users
FROM fact_user_actions
WHERE year = 2023 AND month = 9
GROUP BY region;

-- Top Content by Play Count
SELECT d.title, COUNT(*) AS play_count
FROM fact_user_actions f
JOIN dim_content d ON f.content_id = d.content_id
WHERE f.action = 'play'
GROUP BY d.title
ORDER BY play_count DESC
LIMIT 5;

-- Average Session Length Per Week
SELECT weekofyear(event_timestamp) AS week, 
       avg(length) AS avg_session_length
FROM fact_user_actions f
JOIN dim_content d ON f.content_id = d.content_id
GROUP BY weekofyear(event_timestamp);
