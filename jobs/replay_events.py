"""Atomically publish historical JSON files to the Spark file-stream source."""
import argparse
import json
import os
import time

from stream_paths import checked_id, input_dir, manifest


def publish(path, event):
    payload = json.dumps(event, ensure_ascii=False, sort_keys=True) + '\n'
    if path.exists():
        if path.read_text(encoding='utf-8') != payload:
            raise ValueError('Existing replay file has different content: ' + str(path))
        return False
    temporary = path.with_name('.' + path.name + '.tmp')
    temporary.write_text(payload, encoding='utf-8')
    os.replace(temporary, path)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--through', type=int, choices=(8, 16, 24))
    parser.add_argument('--duplicate', action='store_true')
    parser.add_argument('--delay-ms', type=int, default=80)
    args = parser.parse_args()
    run_id = checked_id(args.run_id)
    if (args.through is None) == (not args.duplicate):
        raise ValueError('Choose exactly one of --through or --duplicate')
    if not 0 <= args.delay_ms <= 1000:
        raise ValueError('Delay must be 0..1000 ms')
    events = manifest(run_id)['events']
    directory = input_dir(run_id)
    if not directory.is_dir():
        raise FileNotFoundError(directory)
    if args.duplicate:
        if sum(p.suffix == '.json' for p in directory.iterdir()) != 24:
            raise ValueError('Publish the 24 unique events before the duplicate')
        event = dict(events[12], event_id=f'{run_id}-duplicate-12')
        changed = publish(directory/'event-duplicate-12.json', event)
        print('DUPLICATE PUBLISHED:', int(changed), 'new JSON file, 2019-10-01 12:00 UTC')
        return
    added = 0
    for index, event in enumerate(events[:args.through]):
        if publish(directory/f'event-{index:02d}.json', event):
            added += 1
            time.sleep(args.delay_ms / 1000)
    count = sum(p.suffix == '.json' for p in directory.iterdir())
    if count != args.through:
        raise ValueError(f'Expected {args.through} source files, found {count}')
    print(f'REPLAY EMITTED: {added} new; {count} total unique-hour JSON files')


if __name__ == '__main__':
    main()
