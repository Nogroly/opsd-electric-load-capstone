"""Retrospective rolling one-hour-ahead baselines on validated DE 2019 HDFS data."""
import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from etl import source_sha256, write_summary
from validate_processed import parse_arguments, run_validation

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs' / 'forecast'
MODELS = ('last_hour', 'last_week_168h')


def build_predictions(source, spark):
    source.select('timestamp_utc', 'load_mw').createOrReplaceTempView('forecast_source')
    return spark.sql((ROOT/'sql/forecast_baselines.sql').read_text(encoding='utf-8'))


def validate_predictions(predictions):
    invalid = F.lit(False)
    for name in ('actual_mw', 'last_hour_mw', 'last_week_168h_mw'):
        invalid = invalid | F.col(name).isNull() | F.isnan(name) | (F.abs(F.col(name)) == float('inf'))
    invalid = (invalid | F.col('lag_1_utc').isNull() | F.col('lag_168_utc').isNull()
               | (F.col('lag_1_utc') != F.col('origin_utc'))
               | (F.col('lag_168_utc') > F.col('origin_utc'))
               | (F.col('origin_utc') >= F.col('target_utc')))
    if predictions.filter(invalid).limit(1).count():
        raise ValueError('Missing/non-finite history or future information in prediction')
    if predictions.groupBy('target_utc').count().filter('count != 1').limit(1).count():
        raise ValueError('Duplicate forecast target timestamp')


def metrics_for(predictions):
    rows = []
    periods = [('validation', 'all', predictions.filter("split = 'validation'")),
               ('test', 'all', predictions.filter("split = 'test'"))]
    periods += [('test', month, predictions.filter(F.date_format('target_utc','yyyy-MM') == month))
                for month in ('2019-10', '2019-11', '2019-12')]
    for split, period, data in periods:
        for model in MODELS:
            error = F.col(model+'_mw') - F.col('actual_mw')
            metrics = data.agg(F.count('*').alias('observations'), F.avg(F.abs(error)).alias('mae_mw'),
                               F.sqrt(F.avg(error*error)).alias('rmse_mw'),
                               F.avg(error).alias('bias_mw')).first().asDict()
            if not metrics['observations']:
                raise ValueError('Empty evaluation period: '+period)
            rows.append(dict(split=split, period=period, model=model, **metrics))
    return rows


def choose_model(metrics):
    validation = [r for r in metrics if r['split']=='validation' and r['period']=='all']
    if len(validation) != 2 or {r['model'] for r in validation} != set(MODELS):
        raise ValueError('Expected validation scores for both fixed baselines')
    return min(validation, key=lambda r: (r['mae_mw'], MODELS.index(r['model'])))['model']


def write_csv(name, rows):
    with (OUT/name).open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    summary_path = str(OUT/'summary.json')
    summary = dict(status='RUNNING', run_id=str(uuid.uuid4()), created_utc=datetime.now(timezone.utc).isoformat())
    write_summary(summary_path, summary)
    (OUT/'index.html').write_text('<meta charset="utf-8"><h1>Forecast running</h1><p>Wait for FORECAST COMPLETED. If the command failed, inspect the log before using results.</p>', encoding='utf-8')
    spark = None
    try:
        spark = (SparkSession.builder.appName('OPSD_Forecast_Baselines').master('local[2]')
                 .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2')
                 .getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        args = parse_arguments(['--report-dir', str(OUT/'quality')])
        if run_validation(args, spark):
            raise ValueError('Source/HDFS validation failed')
        quality = json.loads((OUT/'quality/quality_summary.json').read_text(encoding='utf-8'))
        source = spark.read.parquet(args.hdfs_input).cache()
        predictions = build_predictions(source, spark).cache()
        validate_predictions(predictions)
        counts = {r['split']:r['count'] for r in predictions.groupBy('split').count().collect()}
        if counts != {'validation':720, 'test':2208}:
            raise ValueError('Unexpected evaluation coverage: '+str(counts))
        metrics = metrics_for(predictions)
        selected = choose_model(metrics)
        write_csv('predictions.csv', [r.asDict() for r in predictions.orderBy('target_utc').collect()])
        write_csv('metrics.csv', metrics)
        summary.update(status='PASSED', source_sha256=source_sha256(args.source_csv),
                       hdfs_input=args.hdfs_input, etl_run_id=quality['etl_run_id'],
                       validation_utc=quality['execution_timestamp_utc'],
                       unit='MW', timezone='UTC', horizon_hours=1,
                       history_period='2019-01-01 <= t < 2019-09-01 (5832 hours; no parameter fitting)',
                       validation_period='2019-09-01 <= t < 2019-10-01',
                       test_period='2019-10-01 <= t < 2020-01-01', counts=counts,
                       models={'last_hour':'y(t-1h)', 'last_week_168h':'y(t-168h)'},
                       selection_rule='Lowest validation MAE, tie: last_hour; test never selects model',
                       selected_model=selected,
                       availability_assumption='Actual at t-1h is available at nominal origin t-1h; publication latency and revisions are not modeled',
                       evaluation='Rolling one-hour-ahead backtest with observed history updates, not a fixed-origin multi-month forecast',
                       files={n: source_sha256(str(OUT/n)) for n in ('predictions.csv','metrics.csv')},
                       code_sha256={n: source_sha256(str(ROOT/n)) for n in
                                    ('jobs/forecast_baselines.py','sql/forecast_baselines.sql')})
        write_summary(summary_path, summary)
        print('FORECAST BASELINES PASSED:', counts, 'selected on validation:', selected)
        for row in metrics:
            print(row)
    except Exception as exc:
        summary.update(status='FAILED', error=str(exc))
        write_summary(summary_path, summary)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == '__main__':
    main()
