-- Target t is forecast at nominal origin t-1h, assuming that observation is available.
-- Exact timestamp joins, NOT a row offset that could silently cross missing hours.
-- 168h is a fixed UTC duration; Europe/Berlin wall-clock time may differ across DST.
SELECT t.timestamp_utc AS target_utc,
       t.timestamp_utc - INTERVAL 1 HOUR AS origin_utc,
       p.timestamp_utc AS lag_1_utc,
       w.timestamp_utc AS lag_168_utc,
       t.load_mw AS actual_mw,
       p.load_mw AS last_hour_mw,
       w.load_mw AS last_week_168h_mw,
       CASE WHEN t.timestamp_utc < TIMESTAMP '2019-10-01 00:00:00'
            THEN 'validation' ELSE 'test' END AS split
FROM forecast_source t
LEFT JOIN forecast_source p
  ON p.timestamp_utc = t.timestamp_utc - INTERVAL 1 HOUR
LEFT JOIN forecast_source w
  ON w.timestamp_utc = t.timestamp_utc - INTERVAL 168 HOURS
WHERE t.timestamp_utc >= TIMESTAMP '2019-09-01 00:00:00'
  AND t.timestamp_utc < TIMESTAMP '2020-01-01 00:00:00'
