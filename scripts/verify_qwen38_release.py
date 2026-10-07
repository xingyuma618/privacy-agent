#!/usr/bin/env python3
"""Check stored and decompressed checksums, JSON records, and synthesis totals."""
import collections
import concurrent.futures
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREFIX = 'experiments/qwen38_27b_20261004/'

def verify(entry):
    path = ROOT / entry['stored_path']
    stored_hash = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1048576), b''):
            stored_hash.update(chunk)
    assert stored_hash.hexdigest() == entry['stored_sha256'], path
    h = hashlib.sha256()
    count = 0
    statuses = collections.Counter()
    keys = []
    with gzip.open(path, 'rb') as stream:
        if entry['path'].endswith('.jsonl'):
            for line in stream:
                h.update(line)
                if line.strip():
                    row = json.loads(line)
                    count += 1
                    if entry.get('cache_kind'):
                        statuses[row.get('status', 'label')] += 1
                        keys.append(row['case_key'])
        else:
            content = stream.read()
            h.update(content)
            row = json.loads(content)
            count = len(row) if isinstance(row, list) else None
    assert h.hexdigest() == entry['sha256'], path
    assert count == entry['records'], (path, count, entry['records'])
    return entry, count, statuses, keys

def main():
    entries = [e for e in json.loads((ROOT / 'data_manifest.json').read_text()) if e['path'].startswith(PREFIX)]
    groups = collections.defaultdict(collections.Counter)
    all_keys = collections.defaultdict(list)
    counts = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for entry, count, statuses, keys in pool.map(verify, entries):
            kind = entry.get('cache_kind')
            if kind:
                groups[kind].update(statuses)
                all_keys[kind].extend(keys)
            counts[entry['path']] = count
    summary = json.loads((ROOT / PREFIX / 'runs/full_parallel100/summary.json').read_text())
    for kind, keys in all_keys.items():
        assert len(keys) == len(set(keys)), kind
        if kind == 'labels':
            assert len(keys) == summary['label_cases']
        else:
            s = summary['methods'][kind]
            assert len(keys) == s['completed'] == 9428
            assert groups[kind] == {'accepted': s['accepted'], 'rejected': s['rejected'], 'infrastructure_error': s['infrastructure_errors']}
            assert counts[PREFIX + f'runs/full_parallel100/{kind}/train.jsonl'] == s['accepted']
    assert set(all_keys['prompt']) == set(all_keys['mcts'])
    print(json.dumps({'validated_artifacts': len(entries), 'cache_totals': dict(groups),
                      'stored_mib': round(sum(e['stored_bytes'] for e in entries) / 1048576, 2),
                      'largest_stored_mib': round(max(e['stored_bytes'] for e in entries) / 1048576, 2)}, indent=2))

if __name__ == '__main__':
    main()
