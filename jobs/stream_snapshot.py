"""Verify each HDFS stream stage against replay source and offline ML outputs."""
import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from stream_paths import ROOT, checked_id, input_dir, local_run, manifest, output_hdfs

STAGES = {'stage08': (8, 8, 8), 'stage16': (16, 16, 8),
          'stage24': (24, 24, 8), 'restart': (24, 24, 0),
          'duplicate': (24, 25, 1)}


def close(a, b):
    return math.isclose(float(a), float(b), rel_tol=0, abs_tol=1e-6)


def timestamp(value):
    return value.strftime('%Y-%m-%d %H:%M:%S')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--stage', required=True, choices=STAGES)
    args = parser.parse_args()
    run_id = checked_id(args.run_id)
    expected_rows, expected_files, expected_input = STAGES[args.stage]
    out = local_run(run_id)
    source = manifest(run_id)['events'][:expected_rows]
    progress = json.loads((out/f'progress_{args.stage}.json').read_text(encoding='utf-8'))
    if progress['input_rows_this_launch'] != expected_input:
        raise ValueError(f"Expected {expected_input} input rows for {args.stage}, got "
                         f"{progress['input_rows_this_launch']}")
    files = [p for p in input_dir(run_id).iterdir() if p.suffix == '.json']
    if len(files) != expected_files:
        raise ValueError(f'Expected {expected_files} JSON files, got {len(files)}')
    with (ROOT/'outputs/ml_forecast/predictions.csv').open(encoding='utf-8', newline='') as f:
        offline = {r['target_utc']: r for r in csv.DictReader(f)}
    spark = (SparkSession.builder.appName('OPSD_Stream_Snapshot_' + args.stage).master('local[2]')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        got = [r.asDict() for r in spark.read.parquet(output_hdfs(run_id))
               .orderBy('observed_utc').collect()]
    finally:
        spark.stop()
    if len(got) != expected_rows or len({timestamp(r['observed_utc']) for r in got}) != expected_rows:
        raise ValueError('HDFS output has missing or duplicate observed timestamps')
    for index, (event, row) in enumerate(zip(source, got)):
        target = timestamp(row['target_utc'])
        old = offline.get(target)
        if (timestamp(row['observed_utc']) != event['timestamp_utc']
                or row['event_id'] != event['event_id']
                or not close(row['observed_mw'], event['load_mw'])
                or old is None
                or not close(row['forecast_next_mw'], old['random_forest_mw'])
                or not close(row['baseline_last_hour_mw'], old['last_hour_mw'])
                or not close(row['baseline_last_week_168h_mw'], old['last_week_168h_mw'])):
            raise ValueError('Replay/HDFS/offline ML mismatch at index ' + str(index))
    canonical = json.dumps([{'observed_utc': timestamp(r['observed_utc']),
                             'observed_mw': r['observed_mw'],
                             'target_utc': timestamp(r['target_utc']),
                             'forecast_next_mw': r['forecast_next_mw']} for r in got],
                           sort_keys=True, separators=(',', ':'))
    rows_sha256 = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    by_target = {timestamp(r['target_utc']): r['forecast_next_mw'] for r in got}
    display = []
    errors = []
    for row in got:
        observed_utc = timestamp(row['observed_utc'])
        prior = by_target.get(observed_utc)
        error = None if prior is None else row['observed_mw'] - prior
        if error is not None:
            errors.append(abs(error))
        display.append({'observed_utc': observed_utc, 'actual_mw': row['observed_mw'],
                        'forecast_for_observed_mw': prior,
                        'error_actual_minus_forecast_mw': error,
                        'next_hour_utc': timestamp(row['target_utc']),
                        'forecast_next_mw': row['forecast_next_mw']})
    columns = list(display[0])
    for path in (out/f'events_{args.stage}.csv', ROOT/'outputs/streaming/events.csv'):
        with path.open('w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(display)
    latest = got[-1]
    status = {'status': 'PASSED', 'run_id': run_id, 'stage': args.stage,
              'created_utc': datetime.now(timezone.utc).isoformat(),
              'purpose': '24-hour historical replay of 2019-10-01, not live data',
              'emitted_files': expected_files, 'spark_input_rows_this_launch': expected_input,
              'unique_hdfs_rows': len(got), 'observed_residuals': len(errors),
              'observed_mae_mw': sum(errors)/len(errors) if errors else None,
              'rows_sha256': rows_sha256,
              'latest_observed_utc': timestamp(latest['observed_utc']),
              'latest_observed_mw': latest['observed_mw'],
              'next_hour_utc': timestamp(latest['target_utc']),
              'forecast_next_mw': latest['forecast_next_mw'],
              'hdfs_output': output_hdfs(run_id),
              'hdfs_checkpoint': progress['hdfs_checkpoint'],
              'offline_forecasts_match': True}
    (out/f'snapshot_{args.stage}.json').write_text(json.dumps(status, indent=2), encoding='utf-8')
    (ROOT/'outputs/streaming/latest.json').write_text(json.dumps(status, indent=2), encoding='utf-8')
    print('SNAPSHOT PASSED:', args.stage, len(got), 'unique HDFS rows,',
          expected_files, 'source files, next forecast', round(status['forecast_next_mw'], 2), 'MW')


if __name__ == '__main__':
    main()
