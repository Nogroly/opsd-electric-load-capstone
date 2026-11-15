"""Check progressive ingest, restart and duplicate suppression evidence."""
import argparse
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STAGES = ('stage08', 'stage16', 'stage24', 'restart', 'duplicate')
COUNTS = (8, 16, 24, 24, 24)
INPUTS = (8, 8, 8, 0, 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    run_id = parser.parse_args().run_id
    out = ROOT/'outputs/streaming/runs'/run_id
    snapshots = [json.loads((out/f'snapshot_{stage}.json').read_text(encoding='utf-8'))
                 for stage in STAGES]
    progress = [json.loads((out/f'progress_{stage}.json').read_text(encoding='utf-8'))
                for stage in STAGES]
    assert all(s['status'] == 'PASSED' and s['run_id'] == run_id for s in snapshots)
    assert [s['unique_hdfs_rows'] for s in snapshots] == list(COUNTS)
    assert [s['spark_input_rows_this_launch'] for s in snapshots] == list(INPUTS)
    assert [p['input_rows_this_launch'] for p in progress] == list(INPUTS)
    assert sum(INPUTS) == 25 and snapshots[-1]['emitted_files'] == 25
    assert len({s['rows_sha256'] for s in snapshots[2:]}) == 1
    assert all(s['offline_forecasts_match'] for s in snapshots)
    with (ROOT/'outputs/streaming/events.csv').open(encoding='utf-8', newline='') as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 24 and len({r['observed_utc'] for r in rows}) == 24
    assert sum(bool(r['forecast_for_observed_mw']) for r in rows) == 23
    result = {'status': 'PASSED', 'run_id': run_id,
              'emitted_json_files': 25, 'spark_input_rows': 25,
              'unique_hdfs_rows': 24, 'forecast_comparisons': 24,
              'restart_added_rows': 0, 'duplicate_added_rows': 0,
              'hdfs_rows_unchanged_after_restart_and_duplicate': True,
              'checks': ['three progressive stages', 'source vs HDFS rows',
                         'stream predictions vs offline ML', 'checkpoint restart',
                         'duplicate timestamp after restart']}
    (out/'checks.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    (ROOT/'outputs/streaming/checks.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print('STREAM VERIFICATION PASSED: 25 files read, 24 unique HDFS rows, restart and duplicate safe')


if __name__ == '__main__':
    main()
