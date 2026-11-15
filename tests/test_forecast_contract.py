"""Focused regressions: exact-time history, leakage and validation-only selection."""
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'jobs'))
from forecast_baselines import build_predictions, validate_predictions, choose_model
from pyspark.sql import SparkSession, functions as F


class ForecastContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]').appName('ForecastContract')
                     .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.base = datetime(2019,8,25)
        cls.target = datetime(2019,9,1)
        cls.rows = [(cls.base+timedelta(hours=i),float(10000+i*i)) for i in range(216)]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def source(self, rows=None):
        return self.spark.createDataFrame(self.rows if rows is None else rows, 'timestamp_utc timestamp, load_mw double')

    def test_first_validation_hour_uses_pre_split_history(self):
        pred = build_predictions(self.source(), self.spark)
        row = pred.orderBy('target_utc').first()
        self.assertEqual(row['target_utc'], self.target)
        self.assertEqual(row['last_hour_mw'],10000+167**2)
        self.assertEqual(row['last_week_168h_mw'],10000)
        validate_predictions(pred)

    def test_changing_target_and_future_does_not_change_forecast_at_target(self):
        before = build_predictions(self.source(),self.spark).orderBy('target_utc').first()
        changed = [(t,v+999999 if t>=self.target else v) for t,v in self.rows]
        after = build_predictions(self.source(changed),self.spark).orderBy('target_utc').first()
        for name in ('last_hour_mw','last_week_168h_mw'):
            self.assertEqual(before[name],after[name])
        self.assertNotEqual(before['actual_mw'],after['actual_mw'])

    def test_missing_exact_hour_is_rejected_not_shifted(self):
        missing = [r for r in self.rows if r[0]!=self.target-timedelta(hours=1)]
        with self.assertRaisesRegex(ValueError,'Missing'):
            validate_predictions(build_predictions(self.source(missing),self.spark))

    def test_duplicate_history_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            validate_predictions(build_predictions(self.source(self.rows+[self.rows[167]]),self.spark))

    def test_nonfinite_history_is_rejected(self):
        rows = [(t,float('inf') if t==self.base else v) for t,v in self.rows]
        with self.assertRaisesRegex(ValueError,'non-finite'):
            validate_predictions(build_predictions(self.source(rows),self.spark))

    def test_test_scores_cannot_change_selection(self):
        scores = [dict(split='validation',period='all',model='last_hour',mae_mw=10),
                  dict(split='validation',period='all',model='last_week_168h',mae_mw=20),
                  dict(split='test',period='all',model='last_hour',mae_mw=99999),
                  dict(split='test',period='all',model='last_week_168h',mae_mw=0)]
        self.assertEqual(choose_model(scores),'last_hour')
        scores[1]['mae_mw']=10
        self.assertEqual(choose_model(scores),'last_hour')


if __name__ == '__main__':
    unittest.main(verbosity=2)
