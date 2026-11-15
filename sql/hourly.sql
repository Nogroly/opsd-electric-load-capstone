SELECT hour(timestamp_local) AS hour_berlin, count(*) AS observations,
       avg(load_mw) AS avg_mw, min(load_mw) AS min_mw, max(load_mw) AS max_mw
FROM loads GROUP BY hour(timestamp_local) ORDER BY hour_berlin
