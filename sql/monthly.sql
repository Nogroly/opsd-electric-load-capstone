SELECT date_format(timestamp_utc, 'yyyy-MM') AS month_utc,
       count(*) AS observations,
       dayofmonth(last_day(min(timestamp_utc))) * 24 AS expected_observations,
       avg(load_mw) AS avg_mw, min(load_mw) AS min_mw, max(load_mw) AS max_mw
FROM loads GROUP BY date_format(timestamp_utc, 'yyyy-MM') ORDER BY month_utc
