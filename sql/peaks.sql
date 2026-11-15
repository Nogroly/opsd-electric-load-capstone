SELECT timestamp_utc, timestamp_local AS timestamp_berlin, load_mw
FROM loads ORDER BY load_mw DESC, timestamp_utc ASC LIMIT 10
