SELECT COUNT(*) 
FROM green_taxi_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;

select max(tpep_pickup_datetime)
  FROM yellow_taxi_trips 

SELECT 
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS max_distance
FROM green_taxi_trips
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;


SELECT 
    g."PULocationID",
    SUM(g.total_amount) AS total_amount_sum,
    COUNT(*) AS trip_count
FROM green_taxi_trips g
WHERE DATE(g.lpep_pickup_datetime) = '2025-11-18'
GROUP BY g."PULocationID"
ORDER BY total_amount_sum DESC
LIMIT 1;

SELECT 
    g."DOLocationID",
    MAX(g.tip_amount) AS tip_amount_sum
FROM green_taxi_trips g
WHERE g."PULocationID" = 74
GROUP BY g."DOLocationID"
ORDER BY tIP_amount_sum DESC
LIMIT 1;
