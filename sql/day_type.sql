SELECT CASE WHEN dayofweek(timestamp_local) IN (1,7) THEN 'weekend' ELSE 'weekday' END AS day_type,
       count(*) AS observations, avg(load_mw) AS avg_mw,
       min(load_mw) AS min_mw, max(load_mw) AS max_mw
FROM loads
GROUP BY CASE WHEN dayofweek(timestamp_local) IN (1,7) THEN 'weekend' ELSE 'weekday' END
ORDER BY day_type
