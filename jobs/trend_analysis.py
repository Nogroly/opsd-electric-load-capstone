"""Auditable within-2019 trend and hour-of-week analysis from validated HDFS Parquet."""
import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F
from etl import source_sha256, write_summary

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs' / 'trends'
HDFS = 'hdfs://namenode:8020/bigdata/opsd/processed/de_2019'


def write_csv(name, rows, columns):
    with (OUT / name).open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    summary_path = str(OUT / 'summary.json')
    summary = {'status': 'RUNNING', 'run_id': str(uuid.uuid4()),
               'created_utc': datetime.now(timezone.utc).isoformat()}
    write_summary(summary_path, summary)
    spark = None
    try:
        quality = json.loads((ROOT / 'outputs/quality/quality_summary.json').read_text(encoding='utf-8'))
        etl = json.loads((ROOT / 'outputs/quality/etl_summary.json').read_text(encoding='utf-8'))
        analytics = json.loads((ROOT / 'outputs/analytics/summary.json').read_text(encoding='utf-8'))
        if not (quality['overall_status'] == 'PASSED' and etl['status'] == 'SUCCESS'
                and analytics['status'] == 'PASSED'
                and quality['etl_run_id'] == etl['run_id'] == analytics['etl_run_id']):
            raise ValueError('Plan A evidence or ETL lineage is not current')

        spark = (SparkSession.builder.appName('OPSD_Trend_Analysis').master('local[2]')
                 .config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        source = spark.read.parquet(HDFS).select('timestamp_utc', 'load_mw').cache()
        if source.count() != 8760 or source.select('timestamp_utc').distinct().count() != 8760:
            raise ValueError('HDFS snapshot is not the expected 8760 unique UTC hours')

        daily_base = (source.groupBy(F.to_date('timestamp_utc').alias('day_utc'))
                      .agg(F.count('*').alias('observations'), F.avg('load_mw').alias('avg_mw')))
        w7 = Window.orderBy('day_utc').rowsBetween(-6, 0)
        w30 = Window.orderBy('day_utc').rowsBetween(-29, 0)
        daily = (daily_base.withColumn('history_days_7', F.count('*').over(w7))
                 .withColumn('ma7_mw', F.when(F.col('history_days_7') == 7,
                                               F.avg('avg_mw').over(w7)))
                 .withColumn('history_days_30', F.count('*').over(w30))
                 .withColumn('ma30_mw', F.when(F.col('history_days_30') == 30,
                                                F.avg('avg_mw').over(w30)))
                 .orderBy('day_utc'))
        daily_rows = [r.asDict() for r in daily.collect()]
        if len(daily_rows) != 365 or any(r['observations'] != 24 for r in daily_rows):
            raise ValueError('Daily trend coverage is incomplete')
        daily_columns = ['day_utc', 'observations', 'avg_mw', 'history_days_7',
                         'ma7_mw', 'history_days_30', 'ma30_mw']
        write_csv('daily_trend.csv', daily_rows, daily_columns)

        local = (source.withColumn('timestamp_local', F.from_utc_timestamp('timestamp_utc', 'Europe/Berlin'))
                 .withColumn('hour_local', F.hour('timestamp_local'))
                 .withColumn('weekday_local', (F.dayofweek('timestamp_local') + 5) % 7))
        hour_week = (local.groupBy('weekday_local', 'hour_local')
                     .agg(F.count('*').alias('observations'), F.avg('load_mw').alias('avg_mw'))
                     .orderBy('weekday_local', 'hour_local'))
        hour_rows = [r.asDict() for r in hour_week.collect()]
        if len(hour_rows) != 168 or sum(r['observations'] for r in hour_rows) != 8760:
            raise ValueError('Hour-of-week coverage is incomplete')
        write_csv('hour_week.csv', hour_rows,
                  ['weekday_local', 'hour_local', 'observations', 'avg_mw'])

        month_hour = (local.withColumn('month_utc', F.month('timestamp_utc'))
                      .groupBy('month_utc', 'hour_local')
                      .agg(F.count('*').alias('observations'), F.avg('load_mw').alias('avg_mw'))
                      .orderBy('month_utc', 'hour_local'))
        month_hour_rows = [r.asDict() for r in month_hour.collect()]
        if len(month_hour_rows) != 288 or sum(r['observations'] for r in month_hour_rows) != 8760:
            raise ValueError('Month-hour coverage is incomplete')
        write_csv('month_hour.csv', month_hour_rows,
                  ['month_utc', 'hour_local', 'observations', 'avg_mw'])

        reference = (local.filter(F.col('timestamp_utc') < F.lit('2019-09-01 00:00:00').cast('timestamp'))
                     .groupBy('weekday_local', 'hour_local')
                     .agg(F.avg('load_mw').alias('reference_mw'), F.count('*').alias('reference_hours')))
        deviations = (local.filter(F.col('timestamp_utc') >= F.lit('2019-09-01 00:00:00').cast('timestamp'))
                      .join(reference, ['weekday_local', 'hour_local'])
                      .select('timestamp_utc', 'weekday_local', 'hour_local', 'load_mw',
                              'reference_mw', 'reference_hours',
                              (F.col('load_mw') - F.col('reference_mw')).alias('difference_mw')))
        if deviations.count() != 2928:
            raise ValueError('Reference comparison must cover 2928 Sep-Dec hours')
        top_high = [dict(direction='above_reference', **r.asDict()) for r in
                    deviations.orderBy(F.desc('difference_mw')).limit(10).collect()]
        top_low = [dict(direction='below_reference', **r.asDict()) for r in
                   deviations.orderBy('difference_mw').limit(10).collect()]
        deviation_rows = top_high + top_low
        write_csv('largest_deviations.csv', deviation_rows,
                  ['direction', 'timestamp_utc', 'weekday_local', 'hour_local',
                   'load_mw', 'reference_mw', 'reference_hours', 'difference_mw'])

        valid30 = [r for r in daily_rows if r['ma30_mw'] is not None]
        low30 = min(valid30, key=lambda r: r['ma30_mw'])
        high30 = max(valid30, key=lambda r: r['ma30_mw'])
        monthly = []
        for month in range(1, 13):
            items = [r for r in daily_rows if r['day_utc'].month == month]
            monthly.append({'month_utc': month,
                            'avg_mw': sum(r['avg_mw'] for r in items) / len(items)})
        month_low = min(monthly, key=lambda r: r['avg_mw'])
        month_high = max(monthly, key=lambda r: r['avg_mw'])
        ma30_changes = [{'start_day_utc': str(daily_rows[i-30]['day_utc']),
                         'end_day_utc': str(daily_rows[i]['day_utc']),
                         'change_mw': daily_rows[i]['ma30_mw'] - daily_rows[i-30]['ma30_mw']}
                        for i in range(59, len(daily_rows))]
        strongest_rise = max(ma30_changes, key=lambda r: r['change_mw'])
        strongest_fall = min(ma30_changes, key=lambda r: r['change_mw'])
        hour_high = max(hour_rows, key=lambda r: r['avg_mw'])
        hour_low = min(hour_rows, key=lambda r: r['avg_mw'])
        summary.update(status='PASSED', etl_run_id=etl['run_id'], hdfs_input=HDFS,
                       source_sha256=etl['source_sha256'], unit='MW',
                       day_timezone='UTC', behavior_timezone='Europe/Berlin',
                       rows=8760, days=365, hour_week_groups=168, month_hour_groups=288,
                       reference_period='2019-01-01 <= t < 2019-09-01',
                       comparison_period='2019-09-01 <= t < 2020-01-01',
                       reference_rule='Average of preceding Jan-Aug observations with the same Europe/Berlin weekday and hour',
                       ma30_low={'day_utc': str(low30['day_utc']), 'mw': low30['ma30_mw']},
                       ma30_high={'day_utc': str(high30['day_utc']), 'mw': high30['ma30_mw']},
                       month_low=month_low, month_high=month_high,
                       strongest_ma30_rise_30d=strongest_rise,
                       strongest_ma30_fall_30d=strongest_fall,
                       hour_week_low={'weekday_local': hour_low['weekday_local'],
                                      'hour_local': hour_low['hour_local'], 'mw': hour_low['avg_mw']},
                       hour_week_high={'weekday_local': hour_high['weekday_local'],
                                       'hour_local': hour_high['hour_local'], 'mw': hour_high['avg_mw']},
                       files={name: source_sha256(str(OUT / name)) for name in
                              ('daily_trend.csv', 'hour_week.csv', 'month_hour.csv',
                               'largest_deviations.csv')},
                       code_sha256={'jobs/trend_analysis.py': source_sha256(__file__)})
        write_summary(summary_path, summary)
        print('TREND ANALYSIS PASSED: 365 days, 168 hour-of-week groups, 288 month-hour groups, 2928 reference comparisons')
        print('30-day range:', summary['ma30_low'], summary['ma30_high'])
    except Exception as exc:
        summary.update(status='FAILED', error=str(exc))
        write_summary(summary_path, summary)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    main()
