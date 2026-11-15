"""Independent row/metric reconciliation against verified baseline predictions."""
import csv
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/ml_forecast'
MODELS = ['random_forest', 'last_hour', 'last_week_168h']


def read_csv(path):
    with path.open(encoding='utf-8', newline='') as handle:
        return list(csv.DictReader(handle))


def close(a, b, tolerance=1e-6):
    return math.isclose(float(a), float(b), rel_tol=0, abs_tol=tolerance)


def main():
    summary = json.loads((OUT/'summary.json').read_text(encoding='utf-8'))
    baseline = json.loads((ROOT/'outputs/forecast/summary.json').read_text(encoding='utf-8'))
    assert summary['status'] == baseline['status'] == 'PASSED'
    assert summary['etl_run_id'] == baseline['etl_run_id']
    assert summary['baseline_run_id'] == baseline['run_id']
    assert summary['source_sha256'] == baseline['source_sha256']
    assert summary['counts'] == {'train': 5664, 'validation': 720, 'test': 2208}
    for name, digest in summary['files'].items():
        assert hashlib.sha256((OUT/name).read_bytes()).hexdigest() == digest, name
    predictions = read_csv(OUT/'predictions.csv')
    baseline_rows = read_csv(ROOT/'outputs/forecast/predictions.csv')
    assert len(predictions) == len(baseline_rows) == 2928
    assert [r['target_utc'] for r in predictions] == [r['target_utc'] for r in baseline_rows]
    assert len({r['target_utc'] for r in predictions}) == 2928
    assert sum(r['split']=='validation' for r in predictions)==720
    assert sum(r['split']=='test' for r in predictions)==2208
    for current, old in zip(predictions, baseline_rows):
        assert current['split'] == old['split']
        assert close(current['actual_mw'], old['actual_mw'])
        assert close(current['last_hour_mw'], old['last_hour_mw'])
        assert close(current['last_week_168h_mw'], old['last_week_168h_mw'])
        assert math.isfinite(float(current['random_forest_mw']))
        assert 0 <= float(current['hour_utc']) <= 23
        assert 0 <= float(current['weekday_utc']) <= 6
    metric_rows = read_csv(OUT/'metrics.csv')
    assert len(metric_rows)==15
    expected_keys={(s,p,m) for s,p in [('validation','all'),('test','all'),
                   ('test','2019-10'),('test','2019-11'),('test','2019-12')] for m in MODELS}
    assert {(r['split'],r['period'],r['model']) for r in metric_rows} == expected_keys
    buckets=defaultdict(list)
    for r in predictions:
        buckets[(r['split'],'all')].append(r)
        if r['split']=='test':
            buckets[('test',r['target_utc'][:7])].append(r)
    for metric in metric_rows:
        subset=buckets[(metric['split'],metric['period'])]
        errors=[float(r[metric['model']+'_mw'])-float(r['actual_mw']) for r in subset]
        assert int(metric['observations'])==len(subset)
        assert close(metric['mae_mw'],sum(abs(x) for x in errors)/len(errors))
        assert close(metric['rmse_mw'],math.sqrt(sum(x*x for x in errors)/len(errors)))
        assert close(metric['bias_mw'],sum(errors)/len(errors))
    selected=min(enumerate(summary['candidates']),
                 key=lambda pair:(pair[1]['validation_mae_mw'],pair[0]))[1]
    assert selected==summary['selected_candidate']
    rf_validation=next(r for r in metric_rows if (r['split'],r['period'],r['model']) ==
                       ('validation','all','random_forest'))
    assert close(selected['validation_mae_mw'],rf_validation['mae_mw'])
    importance=json.loads((OUT/'feature_importance.json').read_text(encoding='utf-8'))
    assert set(importance)==set(summary['features'])
    assert close(sum(importance.values()),1.0)
    verification={'status':'PASSED','ml_run_id':summary['run_id'],
                  'baseline_run_id':baseline['run_id'],
                  'targets_checked':2928,'metric_rows_checked':15,
                  'same_targets_and_baselines':'PASSED','selection_check':'PASSED',
                  'files':summary['files']}
    (OUT/'verification.json').write_text(json.dumps(verification,indent=2),encoding='utf-8')
    print('ML VERIFICATION PASSED: 2928 exact baseline targets, 15 metric rows')


if __name__=='__main__':
    main()
