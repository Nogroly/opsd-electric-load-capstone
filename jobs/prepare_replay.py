"""Extract a fixed 24-hour historical replay from validated HDFS Parquet."""
import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

from stream_paths import ROOT, SOURCE_HDFS, checked_id, input_dir, local_run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    run_id = checked_id(parser.parse_args().run_id)
    out = local_run(run_id)
    if out.exists() or input_dir(run_id).exists():
        raise FileExistsError('Replay run ID already exists: ' + run_id)
    quality = json.loads((ROOT/'outputs/quality/quality_summary.json').read_text(encoding='utf-8'))
    etl = json.loads((ROOT/'outputs/quality/etl_summary.json').read_text(encoding='utf-8'))
    ml = json.loads((ROOT/'outputs/ml_forecast/summary.json').read_text(encoding='utf-8'))
    ml_proof = json.loads((ROOT/'outputs/ml_forecast/verification.json').read_text(encoding='utf-8'))
    if not (quality['overall_status'] == 'PASSED' and etl['status'] == 'SUCCESS'
            and ml['status'] == ml_proof['status'] == 'PASSED'
            and etl['run_id'] == quality['etl_run_id'] == ml['etl_run_id']
            and ml['run_id'] == ml_proof['ml_run_id']):
        raise ValueError('Plan A/ML source evidence is not current')
    spark = (SparkSession.builder.appName('OPSD_Prepare_Replay').master('local[2]')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        source = spark.read.parquet(SOURCE_HDFS)
        rows = (source.filter((F.col('timestamp_utc') >= F.lit('2019-10-01 00:00:00').cast('timestamp')) &
                              (F.col('timestamp_utc') < F.lit('2019-10-02 00:00:00').cast('timestamp')))
                .select('timestamp_utc', 'load_mw').orderBy('timestamp_utc').collect())
        events = [{'event_id': f'{run_id}-{index:02d}',
                   'timestamp_utc': row['timestamp_utc'].strftime('%Y-%m-%d %H:%M:%S'),
                   'load_mw': float(row['load_mw'])}
                  for index, row in enumerate(rows)]
        if (len(events) != 24 or len({e['timestamp_utc'] for e in events}) != 24
                or events[0]['timestamp_utc'] != '2019-10-01 00:00:00'
                or events[-1]['timestamp_utc'] != '2019-10-01 23:00:00'
                or any(not 0 < e['load_mw'] < 200000 for e in events)):
            raise ValueError('Expected 24 valid, consecutive October 1 UTC hours')
        out.mkdir(parents=True)
        input_dir(run_id).mkdir(parents=True)
        data = {'run_id': run_id, 'created_utc': datetime.now(timezone.utc).isoformat(),
                'source': SOURCE_HDFS, 'source_sha256': etl['source_sha256'],
                'etl_run_id': etl['run_id'], 'ml_run_id': ml['run_id'],
                'purpose': 'Historical 2019-10-01 UTC replay, not a live meter feed',
                'events': events}
        (out/'source_events.json').write_text(json.dumps(data, indent=2), encoding='utf-8')
        print('REPLAY PREPARED:', run_id, len(events), 'HDFS rows')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
