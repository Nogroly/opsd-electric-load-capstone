"""One-hour-ahead Spark ML regression, compared on the exact baseline targets."""
import csv
import json
import math
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, functions as F

from etl import source_sha256, write_summary

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs' / 'ml_forecast'
HDFS = 'hdfs://namenode:8020/bigdata/opsd/processed/de_2019'
FEATURES = ['lag_1_mw', 'lag_24_mw', 'lag_168_mw', 'hour_utc', 'weekday_utc']
MODELS = ['random_forest', 'last_hour', 'last_week_168h']
CANDIDATES = [(20, 5), (50, 7)]


def write_csv(name, rows, columns):
    with (OUT / name).open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def score(data, prediction_col):
    error = F.col(prediction_col) - F.col('actual_mw')
    result = data.agg(F.count('*').alias('observations'),
                      F.avg(F.abs(error)).alias('mae_mw'),
                      F.sqrt(F.avg(error * error)).alias('rmse_mw'),
                      F.avg(error).alias('bias_mw')).first().asDict()
    if not result['observations'] or not all(math.isfinite(result[k]) for k in
                                             ('mae_mw', 'rmse_mw', 'bias_mw')):
        raise ValueError('Empty or non-finite model score')
    return result


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    summary_path = str(OUT / 'summary.json')
    summary = {'status': 'RUNNING', 'run_id': str(uuid.uuid4()),
               'created_utc': datetime.now(timezone.utc).isoformat()}
    write_summary(summary_path, summary)
    spark = None
    try:
        etl = json.loads((ROOT / 'outputs/quality/etl_summary.json').read_text(encoding='utf-8'))
        quality = json.loads((ROOT / 'outputs/quality/quality_summary.json').read_text(encoding='utf-8'))
        baseline = json.loads((ROOT / 'outputs/forecast/summary.json').read_text(encoding='utf-8'))
        proof = json.loads((ROOT / 'outputs/forecast/verification.json').read_text(encoding='utf-8'))
        if not (etl['status'] == 'SUCCESS' and quality['overall_status'] == 'PASSED'
                and baseline['status'] == proof['status'] == 'PASSED'
                and etl['run_id'] == quality['etl_run_id'] == baseline['etl_run_id']
                and baseline['run_id'] == proof['forecast_run_id']
                and baseline['source_sha256'] == etl['source_sha256']):
            raise ValueError('Baseline/ETL evidence or lineage is not valid')
        for name, digest in baseline['files'].items():
            if source_sha256(str(ROOT / 'outputs/forecast' / name)) != digest:
                raise ValueError('Baseline file changed: ' + name)

        spark = (SparkSession.builder.appName('OPSD_RF_One_Hour_Forecast').master('local[2]')
                 .config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        source = spark.read.parquet(HDFS).select('timestamp_utc', 'load_mw').cache()
        if source.count() != 8760 or source.select('timestamp_utc').distinct().count() != 8760:
            raise ValueError('HDFS snapshot is not the expected 8760 unique UTC hours')
        source.createOrReplaceTempView('ml_source')
        joined = spark.sql("""
            SELECT t.timestamp_utc AS target_utc, t.load_mw AS actual_mw,
                   p1.load_mw AS lag_1_mw, p24.load_mw AS lag_24_mw,
                   p168.load_mw AS lag_168_mw
            FROM ml_source t
            JOIN ml_source p1 ON p1.timestamp_utc = t.timestamp_utc - INTERVAL 1 HOUR
            JOIN ml_source p24 ON p24.timestamp_utc = t.timestamp_utc - INTERVAL 24 HOURS
            JOIN ml_source p168 ON p168.timestamp_utc = t.timestamp_utc - INTERVAL 168 HOURS
            WHERE t.timestamp_utc >= TIMESTAMP '2019-01-08 00:00:00'
              AND t.timestamp_utc < TIMESTAMP '2020-01-01 00:00:00'
        """)
        prepared = (joined.withColumn('hour_utc', F.hour('target_utc').cast('double'))
                    .withColumn('weekday_utc', ((F.dayofweek('target_utc') + 5) % 7).cast('double')))
        prepared = VectorAssembler(inputCols=FEATURES, outputCol='features').transform(prepared).cache()
        train = prepared.filter(F.col('target_utc') < F.lit('2019-09-01 00:00:00').cast('timestamp')).cache()
        validation = prepared.filter((F.col('target_utc') >= F.lit('2019-09-01 00:00:00').cast('timestamp')) &
                                     (F.col('target_utc') < F.lit('2019-10-01 00:00:00').cast('timestamp'))).cache()
        test = prepared.filter(F.col('target_utc') >= F.lit('2019-10-01 00:00:00').cast('timestamp')).cache()
        counts = {'train': train.count(), 'validation': validation.count(), 'test': test.count()}
        if counts != {'train': 5664, 'validation': 720, 'test': 2208}:
            raise ValueError('Unexpected time-split coverage: ' + str(counts))
        for field in ['actual_mw'] + FEATURES:
            if prepared.filter(F.col(field).isNull() | F.isnan(field)).limit(1).count():
                raise ValueError('Missing/non-numeric value in ' + field)

        candidate_scores = []
        fitted = []
        for trees, depth in CANDIDATES:
            reg = RandomForestRegressor(featuresCol='features', labelCol='actual_mw',
                                        predictionCol='random_forest_mw', numTrees=trees,
                                        maxDepth=depth, seed=42, maxBins=32)
            model = reg.fit(train)
            result = score(model.transform(validation), 'random_forest_mw')
            candidate_scores.append({'num_trees': trees, 'max_depth': depth,
                                     'validation_mae_mw': result['mae_mw'],
                                     'validation_rmse_mw': result['rmse_mw']})
            fitted.append(model)
        selected_index = min(range(len(candidate_scores)),
                             key=lambda i: (candidate_scores[i]['validation_mae_mw'], i))
        selected = fitted[selected_index]
        scored = (selected.transform(validation.unionByName(test))
                  .withColumn('split', F.when(F.col('target_utc') < F.lit('2019-10-01 00:00:00').cast('timestamp'),
                                               'validation').otherwise('test'))
                  .withColumn('last_hour_mw', F.col('lag_1_mw'))
                  .withColumn('last_week_168h_mw', F.col('lag_168_mw'))
                  .select('target_utc', 'split', 'actual_mw', 'random_forest_mw',
                          'last_hour_mw', 'last_week_168h_mw', 'lag_24_mw',
                          'hour_utc', 'weekday_utc').cache())
        if scored.count() != 2928 or scored.select('target_utc').distinct().count() != 2928:
            raise ValueError('Scored targets are incomplete or duplicated')

        metrics = []
        periods = [('validation', 'all', scored.filter("split = 'validation'")),
                   ('test', 'all', scored.filter("split = 'test'"))]
        periods += [('test', month, scored.filter(F.date_format('target_utc', 'yyyy-MM') == month))
                    for month in ('2019-10', '2019-11', '2019-12')]
        for split, period, part in periods:
            for name in MODELS:
                metrics.append(dict(split=split, period=period, model=name,
                                    **score(part, name + '_mw')))

        with (ROOT / 'outputs/forecast/metrics.csv').open(encoding='utf-8') as handle:
            old_metrics = list(csv.DictReader(handle))
        for row in metrics:
            if row['model'] == 'random_forest':
                continue
            match = next((r for r in old_metrics if r['split'] == row['split']
                          and r['period'] == row['period'] and r['model'] == row['model']), None)
            if match is None or row['observations'] != int(match['observations']) or any(
                    abs(row[k] - float(match[k])) > 1e-6 for k in ('mae_mw', 'rmse_mw', 'bias_mw')):
                raise ValueError('Baseline comparison differs from verified forecast: ' + str(row))

        prediction_rows = [r.asDict() for r in scored.orderBy('target_utc').collect()]
        write_csv('predictions.csv', prediction_rows,
                  ['target_utc', 'split', 'actual_mw', 'random_forest_mw', 'last_hour_mw',
                   'last_week_168h_mw', 'lag_24_mw', 'hour_utc', 'weekday_utc'])
        write_csv('metrics.csv', metrics,
                  ['split', 'period', 'model', 'observations', 'mae_mw', 'rmse_mw', 'bias_mw'])
        importances = dict(zip(FEATURES, map(float, selected.featureImportances)))
        (OUT / 'feature_importance.json').write_text(json.dumps(importances, indent=2), encoding='utf-8')
        selected.write().overwrite().save(str(OUT / 'model'))
        summary.update(status='PASSED', etl_run_id=etl['run_id'],
                       baseline_run_id=baseline['run_id'], source_sha256=etl['source_sha256'],
                       hdfs_input=HDFS, unit='MW', timezone='UTC', horizon_hours=1,
                       features=FEATURES, counts=counts, candidates=candidate_scores,
                       selected_candidate=candidate_scores[selected_index], seed=42,
                       train_period='2019-01-08 <= t < 2019-09-01',
                       validation_period='2019-09-01 <= t < 2019-10-01',
                       test_period='2019-10-01 <= t < 2020-01-01',
                       selection_rule='Lowest validation MAE; test never selects hyperparameters',
                       evaluation='Rolling one-hour-ahead with observed prior loads; no publication latency modeled',
                       files={name: source_sha256(str(OUT / name)) for name in
                              ('predictions.csv', 'metrics.csv', 'feature_importance.json')},
                       code_sha256={'jobs/ml_forecast.py': source_sha256(__file__)})
        write_summary(summary_path, summary)
        print('ML FORECAST PASSED:', counts, 'selected', summary['selected_candidate'])
        for row in metrics[:6]:
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
