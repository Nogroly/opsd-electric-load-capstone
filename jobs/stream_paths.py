"""Shared, isolated paths for a 24-hour historical streaming replay."""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_HDFS = 'hdfs://namenode:8020/bigdata/opsd/processed/de_2019'
STREAM_HDFS = 'hdfs://namenode:8020/bigdata/opsd/stream_runs'


def checked_id(run_id):
    if not re.fullmatch(r'[0-9]{8}T[0-9]{9}', run_id):
        raise ValueError('Run ID must be YYYYMMDDTHHMMSSmmm')
    return run_id


def local_run(run_id):
    return ROOT / 'outputs' / 'streaming' / 'runs' / checked_id(run_id)


def input_dir(run_id):
    return ROOT / 'data' / 'replay_runs' / checked_id(run_id) / 'input'


def output_hdfs(run_id):
    return STREAM_HDFS + '/' + checked_id(run_id) + '/parquet'


def checkpoint_hdfs(run_id):
    return STREAM_HDFS + '/' + checked_id(run_id) + '/checkpoint'


def manifest(run_id):
    return json.loads((local_run(run_id) / 'source_events.json').read_text(encoding='utf-8'))
