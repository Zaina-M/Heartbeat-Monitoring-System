-- Count total records
SELECT COUNT(*) AS total_records FROM heartbeat_records;

-- Get records from last 5 minutes
SELECT * FROM heartbeat_records
WHERE timestamp > NOW() - INTERVAL '5 minutes'
ORDER BY timestamp DESC
LIMIT 100;

-- Count anomalies by type
SELECT anomaly_type, COUNT(*) AS count
FROM heartbeat_records
WHERE is_anomaly = TRUE
GROUP BY anomaly_type
ORDER BY count DESC;

-- Average heart rate per customer (last hour)
SELECT customer_id, 
       AVG(heart_rate) AS avg_hr,
       MIN(heart_rate) AS min_hr,
       MAX(heart_rate) AS max_hr,
       COUNT(*) AS readings
FROM heartbeat_records
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY customer_id
ORDER BY avg_hr DESC;

-- Recent anomalies with details
SELECT customer_id, timestamp, heart_rate, anomaly_type
FROM heartbeat_records
WHERE is_anomaly = TRUE
  AND timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC
LIMIT 50;

-- Hourly statistics
SELECT date_trunc('hour', timestamp) AS hour,
       COUNT(*) AS total_readings,
       AVG(heart_rate) AS avg_hr,
       SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count
FROM heartbeat_records
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;

-- Active customers (sent data in last 10 minutes)
SELECT COUNT(DISTINCT customer_id) AS active_customers
FROM heartbeat_records
WHERE timestamp > NOW() - INTERVAL '10 minutes';
