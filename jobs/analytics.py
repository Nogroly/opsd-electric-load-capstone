#!/usr/bin/env python3
"""M3: validate live HDFS data, execute auditable SQL, publish small KPI tables."""
import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from etl import source_sha256, write_summary
from validate_processed import parse_arguments, run_validation

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs' / 'analytics'
TABLES = ('daily', 'monthly', 'hourly', 'day_type', 'peaks')


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = {'status': 'RUNNING', 'run_id': str(uuid.uuid4()),
                'created_utc': datetime.now(timezone.utc).isoformat()}
    manifest_path = str(OUT / 'summary.json')
    write_summary(manifest_path, manifest)
    spark = None
    try:
        spark = (SparkSession.builder.appName('OPSD_PlanA_SQL').master('local[2]')
                 .config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        args = parse_arguments([])
        if run_validation(args, spark) != 0:
            raise ValueError('Fresh HDFS/source validation failed; analytics stopped')
        quality = json.loads(Path(args.report_dir, 'quality_summary.json').read_text())
        df = spark.read.parquet(args.hdfs_input).cache()
        df.createOrReplaceTempView('processed')
        spark.sql("""CREATE OR REPLACE TEMP VIEW loads AS
            SELECT *, from_utc_timestamp(timestamp_utc, 'Europe/Berlin') AS timestamp_local
            FROM processed""")
        results, queries = {}, {}
        for name in TABLES:
            query_path = ROOT / 'sql' / (name + '.sql')
            queries[name] = source_sha256(str(query_path))
            table = spark.sql(query_path.read_text(encoding='utf-8'))
            rows = [r.asDict() for r in table.collect()]
            results[name] = rows
            with (OUT / (name + '.csv')).open('w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=table.columns)
                writer.writeheader()
                writer.writerows(rows)
        for name, expected in [('daily', 365), ('monthly', 12), ('hourly', 24), ('day_type', 2)]:
            assert len(results[name]) == expected, name
            assert sum(r['observations'] for r in results[name]) == 8760, name
        for name in ('daily', 'monthly'):
            assert all(r['observations'] == r['expected_observations'] for r in results[name]), name
        manifest.update(status='PASSED', rows=df.count(), hdfs_input=args.hdfs_input,
                        source_sha256=source_sha256(args.source_csv),
                        etl_run_id=quality['etl_run_id'],
                        validation_utc=quality['execution_timestamp_utc'],
                        scope='2019-01-01T00:00:00Z <= timestamp_utc < 2020-01-01T00:00:00Z',
                        calendar_timezone='UTC', behavior_timezone='Europe/Berlin', unit='MW',
                        query_sha256=queries,
                        files={n+'.csv': source_sha256(str(OUT/(n+'.csv'))) for n in TABLES})
        write_summary(manifest_path, manifest)
        print('ANALYTICS PASSED:', {n: len(r) for n,r in results.items()})
    except Exception as exc:
        manifest.update(status='FAILED', error=str(exc))
        write_summary(manifest_path, manifest)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    main()
