"""Spark Structured Streaming file replay with HDFS Parquet sink and checkpoint."""
import argparse
import json
from datetime import datetime, timezone

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.sql import SparkSession, functions as F, types as T

from stream_paths import (ROOT, SOURCE_HDFS, checked_id, checkpoint_hdfs,
                          input_dir, local_run, manifest, output_hdfs)

FEATURES = ['lag_1_mw', 'lag_24_mw', 'lag_168_mw', 'hour_utc', 'weekday_utc']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--stage', required=True,
                        choices=('stage08', 'stage16', 'stage24', 'restart', 'duplicate'))
    args = parser.parse_args()
    run_id = checked_id(args.run_id)
    meta = manifest(run_id)
    ml = json.loads((ROOT/'outputs/ml_forecast/summary.json').read_text(encoding='utf-8'))
    if ml['status'] != 'PASSED' or ml['run_id'] != meta['ml_run_id'] or ml['features'] != FEATURES:
        raise ValueError('Offline model lineage or feature contract changed')
    spark = (SparkSession.builder.appName('OPSD_Stream_Replay_' + args.stage).master('local[2]')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.streaming.stateStore.maintenanceInterval', '60s')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        model = RandomForestRegressionModel.load(str(ROOT/'outputs/ml_forecast/model'))
        if model.getFeaturesCol() != 'features' or model.getPredictionCol() != 'random_forest_mw':
            raise ValueError('Saved model feature/prediction columns do not match')
        history = spark.read.parquet(SOURCE_HDFS).select('timestamp_utc', 'load_mw')
        # Explicit schema: no inference from potentially incomplete arriving files.
        schema = T.StructType([
            T.StructField('event_id', T.StringType()),
            T.StructField('timestamp_utc', T.StringType()),
            T.StructField('load_mw', T.DoubleType()),
        ])
        incoming = (spark.readStream.schema(schema).option('maxFilesPerTrigger', 4)
                    .json(str(input_dir(run_id))))
        parsed = (incoming.withColumn('observed_utc',
                                     F.to_timestamp('timestamp_utc', 'yyyy-MM-dd HH:mm:ss'))
                  .withColumnRenamed('load_mw', 'observed_mw')
                  .filter(F.col('event_id').isNotNull()
                          & F.col('observed_utc').between(
                              F.lit('2019-10-01 00:00:00').cast('timestamp'),
                              F.lit('2019-10-01 23:00:00').cast('timestamp'))
                          & F.col('observed_mw').between(1.0, 200000.0)))
        # The bounded demo keeps all 24 seen timestamp keys in checkpoint state.
        distinct = parsed.dropDuplicates(['observed_utc'])
        target = distinct.withColumn('target_utc', F.expr('observed_utc + INTERVAL 1 HOUR'))
        h24 = history.select(F.col('timestamp_utc').alias('history_24_utc'),
                             F.col('load_mw').alias('lag_24_mw'))
        h168 = history.select(F.col('timestamp_utc').alias('history_168_utc'),
                              F.col('load_mw').alias('lag_168_mw'))
        # Every lookup is strictly before target_utc; target actual is never a feature.
        joined = (target.join(h24, F.col('history_24_utc') ==
                              F.expr('target_utc - INTERVAL 24 HOURS'), 'inner')
                  .join(h168, F.col('history_168_utc') ==
                        F.expr('target_utc - INTERVAL 168 HOURS'), 'inner')
                  .withColumn('lag_1_mw', F.col('observed_mw'))
                  .withColumn('hour_utc', F.hour('target_utc').cast('double'))
                  .withColumn('weekday_utc',
                              ((F.dayofweek('target_utc') + 5) % 7).cast('double')))
        feature_df = VectorAssembler(inputCols=FEATURES, outputCol='features').transform(joined)
        predictions = (model.transform(feature_df)
                       .select('event_id', 'observed_utc', 'observed_mw', 'target_utc',
                               F.col('random_forest_mw').alias('forecast_next_mw'),
                               F.col('lag_1_mw').alias('baseline_last_hour_mw'),
                               F.col('lag_168_mw').alias('baseline_last_week_168h_mw')))
        query = (predictions.writeStream.format('parquet').outputMode('append')
                 .option('path', output_hdfs(run_id))
                 .option('checkpointLocation', checkpoint_hdfs(run_id))
                 .trigger(availableNow=True).start())
        query.awaitTermination()
        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))
        progress = query.recentProgress
        result = {'run_id': run_id, 'stage': args.stage,
                  'ended_utc': datetime.now(timezone.utc).isoformat(),
                  'input_rows_this_launch': sum(int(p.get('numInputRows', 0)) for p in progress),
                  'microbatches': len(progress),
                  'source': str(input_dir(run_id)),
                  'hdfs_output': output_hdfs(run_id),
                  'hdfs_checkpoint': checkpoint_hdfs(run_id),
                  'progress': progress}
        (local_run(run_id)/f'progress_{args.stage}.json').write_text(
            json.dumps(result, indent=2), encoding='utf-8')
        print('STREAM FINISHED:', args.stage, 'input rows', result['input_rows_this_launch'],
              'microbatches', result['microbatches'])
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
