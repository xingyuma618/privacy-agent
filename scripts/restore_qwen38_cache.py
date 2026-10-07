#!/usr/bin/env python3
"""Restore published JSONL cache shards into per-case synthesis cache files."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREFIX = Path('experiments/qwen38_27b_20261004')

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / PREFIX / 'runs/full_parallel100')
    args = parser.parse_args()
    entries = [e for e in json.loads((ROOT / 'data_manifest.json').read_text())
               if e['path'].startswith(str(PREFIX) + '/') and e.get('cache_kind')]
    counts = {}
    seen = set()
    for e in entries:
        stored = ROOT / e['stored_path']
        if digest(stored) != e['stored_sha256']:
            raise SystemExit(f'Checksum mismatch: {stored}')
        kind = e['cache_kind']
        target_dir = args.output / ('labels' if kind == 'labels' else kind + '/cases')
        target_dir.mkdir(parents=True, exist_ok=True)
        h = hashlib.sha256()
        rows = 0
        with gzip.open(stored, 'rb') as stream:
            for line in stream:
                h.update(line)
                value = json.loads(line)
                key = value['case_key']
                if key != Path(key).name or (kind, key) in seen:
                    raise SystemExit(f'Invalid/duplicate cache key: {kind}/{key}')
                seen.add((kind, key))
                target = target_dir / (key + '.json')
                if target.exists():
                    if json.loads(target.read_text()) != value:
                        raise SystemExit(f'Existing cache differs; move it aside: {target}')
                else:
                    temporary = target.with_suffix('.json.extracting')
                    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
                    temporary.replace(target)
                rows += 1
        if h.hexdigest() != e['sha256'] or rows != e['records']:
            raise SystemExit(f'Invalid shard contents: {stored}')
        counts[kind] = counts.get(kind, 0) + rows
    print(json.dumps({'restored': counts, 'output': str(args.output)}, indent=2))

if __name__ == '__main__':
    main()
