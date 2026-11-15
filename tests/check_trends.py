"""Independent checks against Plan A daily exports and trend output contracts."""
import csv
import hashlib
import json
import math
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/trends'


def rows(path):
    with path.open(encoding='utf-8', newline='') as handle:
        return list(csv.DictReader(handle))


def close(a, b):
    return math.isclose(float(a), float(b), rel_tol=0, abs_tol=1e-6)


def main():
    summary = json.loads((OUT/'summary.json').read_text(encoding='utf-8'))
    assert summary['status'] == 'PASSED'
    quality = json.loads((ROOT/'outputs/quality/quality_summary.json').read_text(encoding='utf-8'))
    assert summary['etl_run_id'] == quality['etl_run_id']
    for name, digest in summary['files'].items():
        assert hashlib.sha256((OUT/name).read_bytes()).hexdigest() == digest, name
    daily = rows(OUT/'daily_trend.csv')
    original = rows(ROOT/'outputs/analytics/daily.csv')
    assert len(daily) == len(original) == 365
    assert [r['day_utc'] for r in daily] == [r['day_utc'] for r in original]
    for i, (a, b) in enumerate(zip(daily, original)):
        assert date.fromisoformat(a['day_utc']) == date(2019, 1, 1) + timedelta(days=i)
        assert int(a['observations']) == int(b['observations']) == 24
        assert close(a['avg_mw'], b['avg_mw'])
        for window, key in ((7, 'ma7_mw'), (30, 'ma30_mw')):
            expected = sum(float(daily[j]['avg_mw']) for j in range(max(0, i-window+1), i+1))/min(i+1, window)
            assert int(a['history_days_'+str(window)]) == min(i+1, window)
            if i+1 < window:
                assert a[key] in ('', 'None')
            else:
                assert close(a[key], expected), (i, key)
    hour_week = rows(OUT/'hour_week.csv')
    assert len(hour_week) == 168
    assert {(int(r['weekday_local']), int(r['hour_local'])) for r in hour_week} == {
        (d, h) for d in range(7) for h in range(24)}
    assert sum(int(r['observations']) for r in hour_week) == 8760
    month_hour = rows(OUT/'month_hour.csv')
    assert len(month_hour) == 288
    assert {(int(r['month_utc']), int(r['hour_local'])) for r in month_hour} == {
        (m, h) for m in range(1, 13) for h in range(24)}
    assert sum(int(r['observations']) for r in month_hour) == 8760
    for key, choose in (('month_low', min), ('month_high', max)):
        monthly = [(m, sum(float(r['avg_mw']) for r in daily if date.fromisoformat(r['day_utc']).month == m)
                    / sum(1 for r in daily if date.fromisoformat(r['day_utc']).month == m))
                   for m in range(1, 13)]
        selected = choose(monthly, key=lambda x: x[1])
        assert summary[key]['month_utc'] == selected[0]
        assert close(summary[key]['avg_mw'], selected[1])
    changes = [(i, float(daily[i]['ma30_mw']) - float(daily[i-30]['ma30_mw']))
               for i in range(59, 365)]
    for key, selected in (('strongest_ma30_rise_30d', max(changes, key=lambda x: x[1])),
                          ('strongest_ma30_fall_30d', min(changes, key=lambda x: x[1]))):
        assert summary[key]['start_day_utc'] == daily[selected[0]-30]['day_utc']
        assert summary[key]['end_day_utc'] == daily[selected[0]]['day_utc']
        assert close(summary[key]['change_mw'], selected[1])
    deviations = rows(OUT/'largest_deviations.csv')
    assert len(deviations) == 20
    assert [r['direction'] for r in deviations] == ['above_reference']*10 + ['below_reference']*10
    assert len({r['timestamp_utc'] for r in deviations}) == 20
    for r in deviations:
        assert close(r['difference_mw'], float(r['load_mw'])-float(r['reference_mw']))
        assert r['timestamp_utc'] >= '2019-09-01'
    verification = {'status': 'PASSED', 'trend_run_id': summary['run_id'],
                    'days_checked': 365, 'hour_week_groups_checked': 168,
                    'month_hour_groups_checked': 288,
                    'deviations_checked': 20, 'daily_crosscheck': 'PASSED',
                    'rolling_windows': 'PASSED'}
    (OUT/'verification.json').write_text(json.dumps(verification, indent=2), encoding='utf-8')
    print('TREND VERIFICATION PASSED: 365 days, 168 hour-week groups, 20 deviations')


if __name__ == '__main__':
    main()
