"""Independent raw-CSV oracle: every forecast, lag timestamp, split, metric and selection."""
import csv
import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT/'outputs/forecast'


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024), b''):
            result.update(chunk)
    return result.hexdigest()


def check(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    summary = json.loads((OUT/'summary.json').read_text())
    report = dict(status='FAILED', forecast_run_id=summary['run_id'])
    try:
        check(summary['status']=='PASSED', 'Forecast job did not pass')
        source_path = ROOT/'data/raw/time_series_60min_singleindex.csv'
        check(digest(source_path)==summary['source_sha256'], 'Source hash mismatch')
        for name, expected in summary['files'].items():
            check(digest(OUT/name)==expected, 'Output hash mismatch: '+name)
        history = {}
        with source_path.open(encoding='utf-8') as f:
            for row in csv.DictReader(f):
                ts = datetime.fromisoformat(row['utc_timestamp'].replace('Z','+00:00'))
                if ts.year == 2019:
                    check(ts not in history, 'Duplicate source time')
                    history[ts] = float(row['DE_load_actual_entsoe_transparency'])
        start, cutoff, end = [datetime(2019,9,1,tzinfo=timezone.utc),
                              datetime(2019,10,1,tzinfo=timezone.utc), datetime(2020,1,1,tzinfo=timezone.utc)]
        expected_times = [start+timedelta(hours=i) for i in range(2928)]
        errors = {}
        with (OUT/'predictions.csv').open() as f:
            rows = list(csv.DictReader(f))
        parse = lambda t: datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        check([parse(r['target_utc']) for r in rows] == expected_times, 'Missing, extra or unordered targets')
        for row, ts in zip(rows, expected_times):
            split = 'validation' if ts < cutoff else 'test'
            check(row['split']==split, 'Wrong split')
            check(parse(row['origin_utc'])==ts-timedelta(hours=1), 'Wrong origin')
            actual = history[ts]
            check(float(row['actual_mw'])==actual, 'Wrong actual')
            for model, lag in [('last_hour',1),('last_week_168h',168)]:
                prior = ts-timedelta(hours=lag)
                check(parse(row['lag_'+str(lag)+'_utc'])==prior, 'Wrong lag time')
                check(prior<=parse(row['origin_utc'])<ts, 'Leakage')
                pred = history[prior]
                check(float(row[model+'_mw'])==pred, 'Prediction mismatch')
                error = pred-actual
                errors.setdefault((split,'all',model),[]).append(error)
                if split=='test':
                    errors.setdefault((split,ts.strftime('%Y-%m'),model),[]).append(error)
        with (OUT/'metrics.csv').open() as f:
            scores = list(csv.DictReader(f))
        check(len(scores)==len(errors)==10, 'Metric count mismatch')
        check({(r['split'],r['period'],r['model']) for r in scores}==set(errors), 'Metric keys mismatch')
        expected_scores = {}
        for row in scores:
            key = row['split'],row['period'],row['model']
            values = errors[key]
            expected = dict(mae_mw=math.fsum(abs(v) for v in values)/len(values),
                            rmse_mw=math.sqrt(math.fsum(v*v for v in values)/len(values)),
                            bias_mw=math.fsum(values)/len(values))
            check(int(row['observations'])==len(values), 'Metric denominator mismatch')
            for name,value in expected.items():
                check(math.isclose(float(row[name]),value,rel_tol=1e-10,abs_tol=1e-7), 'Metric mismatch: '+name)
            expected_scores[key] = expected
        selected = min(('last_hour','last_week_168h'), key=lambda m:expected_scores['validation','all',m]['mae_mw'])
        check(summary['selected_model']==selected, 'Selection must use validation MAE')
        report.update(status='PASSED', targets_checked=2928, predictions_checked=5856,
                      metric_rows_checked=10, validation_targets=720, test_targets=2208,
                      lag_and_origin_checks='PASSED', selection_check='PASSED', files=summary['files'])
        print('FORECAST INDEPENDENT CHECK PASSED:', report)
    except Exception as exc:
        report['error'] = str(exc)
        raise
    finally:
        (OUT/'verification.json').write_text(json.dumps(report,indent=2),encoding='utf-8')


if __name__ == '__main__':
    main()
