"""Independent CSV/zoneinfo oracle for every SQL group, plus DST regressions."""
import csv
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs' / 'analytics'


def main():
    summary = json.loads((OUT / 'summary.json').read_text())
    report = {'status': 'FAILED', 'analytics_run_id': summary['run_id']}
    try:
        assert summary['status'] == 'PASSED'
        groups = {n: defaultdict(list) for n in ('daily', 'monthly', 'hourly', 'day_type')}
        peaks = []
        tz = ZoneInfo('Europe/Berlin')
        with (ROOT / 'data/raw/time_series_60min_singleindex.csv').open(encoding='utf-8') as f:
            for row in csv.DictReader(f):
                utc = datetime.fromisoformat(row['utc_timestamp'].replace('Z', '+00:00'))
                if utc.year != 2019:
                    continue
                local = utc.astimezone(tz)
                value = float(row['DE_load_actual_entsoe_transparency'])
                keys = dict(daily=utc.strftime('%Y-%m-%d'), monthly=utc.strftime('%Y-%m'),
                            hourly=str(local.hour), day_type='weekend' if local.weekday() >= 5 else 'weekday')
                for name, key in keys.items():
                    groups[name][key].append(value)
                peaks.append((value, utc.strftime('%Y-%m-%d %H:%M:%S'), local.strftime('%Y-%m-%d %H:%M:%S')))
        checked = 0
        for name, key_col in [('daily','day_utc'), ('monthly','month_utc'), ('hourly','hour_berlin'), ('day_type','day_type')]:
            with (OUT/(name+'.csv')).open() as f:
                rows = list(csv.DictReader(f))
            assert len(rows) == len(groups[name])
            assert {r[key_col] for r in rows} == set(groups[name])
            for r in rows:
                values = groups[name][r[key_col]]
                assert int(r['observations']) == len(values)
                for key, actual in [('avg_mw', math.fsum(values)/len(values)), ('min_mw', min(values)), ('max_mw', max(values))]:
                    assert math.isclose(float(r[key]), actual, rel_tol=1e-12, abs_tol=1e-7), (name,r)
                checked += 1
        with (OUT/'peaks.csv').open() as f:
            actual = list(csv.DictReader(f))
        expected = sorted(peaks, key=lambda r: (-r[0], r[1]))[:10]
        assert len(actual) == 10
        for r, e in zip(actual, expected):
            assert (float(r['load_mw']), r['timestamp_utc'], r['timestamp_berlin']) == e
        # Spring skips 02:00, autumn repeats 02:00 with distinct UTC instants.
        convert = lambda t: datetime.fromisoformat(t).astimezone(tz)
        assert convert('2019-03-31T00:00:00+00:00').hour == 1
        assert convert('2019-03-31T01:00:00+00:00').hour == 3
        a, b = convert('2019-10-27T00:00:00+00:00'), convert('2019-10-27T01:00:00+00:00')
        assert a.hour == b.hour == 2 and a.utcoffset() != b.utcoffset()
        report.update(status='PASSED', groups_checked=checked, peaks_checked=10, dst_checks='PASSED')
        print('INDEPENDENT CSV ORACLE PASSED:', report)
    finally:
        (OUT/'verification.json').write_text(json.dumps(report, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
