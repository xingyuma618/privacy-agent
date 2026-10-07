#!/usr/bin/env python3
"""Publish an unchanged synthesis snapshot into the release repository."""
import argparse
import concurrent.futures
import gzip
import hashlib
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREFIX = Path('experiments/qwen38_27b_20261004')

def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    dest = ROOT / PREFIX
    dest.mkdir(parents=True, exist_ok=True)
    provenance = {}
    entries = []

    def copy(relative, target=None):
        target = target or relative
        out = dest / target
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / relative, out)
        provenance[str(PREFIX / target)] = {
            'source': str(source / relative), 'published_sha256': sha(out)}

    for directory in ['src', 'tests', 'scripts', 'configs']:
        for path in sorted((source / directory).rglob('*')):
            if path.is_file() and '__pycache__' not in path.parts:
                copy(path.relative_to(source))
    for name in ['README.md', 'EXPERIMENT_NOTES.md', 'requirements.txt', 'data/input_manifest.json']:
        copy(Path(name))
    run = Path('runs/full_parallel100')
    for name in ['manifest.json', 'summary.json', 'cache_provenance.json']:
        copy(run / name)
    for name in ['sglang_comparison.json', 'backend_test_state.json']:
        copy(Path('runs') / name)
    audit = Path('runs/audit_20261004_232200')
    for path in sorted((source / audit).iterdir()):
        if path.is_file() and path.name != 'snapshot.json':
            copy(path.relative_to(source), Path('audit/20261004_232200') / path.name)

    def pack(relative, target=None):
        target = target or relative
        original = source / relative
        out = dest / (str(target) + '.gz')
        out.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        # Fixed mtime/name makes gzip reproducible; source bytes are unchanged.
        with original.open('rb') as stream, out.open('wb') as raw:
            with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0, compresslevel=6) as compressed:
                for line in stream:
                    if target.suffix == '.jsonl' and line.strip():
                        json.loads(line)
                        count += 1
                    compressed.write(line)
        if target.suffix == '.json':
            value = json.loads(original.read_text())
            count = len(value) if isinstance(value, list) else None
        entry = {'path': str(PREFIX / target), 'stored_path': str(out.relative_to(ROOT)),
                 'bytes': original.stat().st_size, 'stored_bytes': out.stat().st_size,
                 'sha256': sha(original), 'stored_sha256': sha(out), 'records': count,
                 'source': str(original)}
        print(f'Packed {target}: {entry["stored_bytes"] / 1048576:.2f} MiB, {count} records', flush=True)
        return [entry]

    def pack_cache(kind):
        directory = source / run / ('labels' if kind == 'labels' else kind + '/cases')
        paths = sorted(directory.glob('*.json'))
        result = []
        status = {}
        seen = set()
        for start in range(0, len(paths), 1000):
            target = run / 'cache_archives' / f'{kind}-{start // 1000:03d}.jsonl'
            out = dest / (str(target) + '.gz')
            out.parent.mkdir(parents=True, exist_ok=True)
            h = hashlib.sha256()
            size = 0
            batch = paths[start:start + 1000]
            with out.open('wb') as raw:
                with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0, compresslevel=6) as compressed:
                    for path in batch:
                        value = json.loads(path.read_text())
                        key = value['case_key']
                        if key in seen or key != path.stem:
                            raise ValueError(f'Invalid/duplicate cache key: {path}')
                        seen.add(key)
                        status[value.get('status', 'label')] = status.get(value.get('status', 'label'), 0) + 1
                        line = (json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n').encode()
                        compressed.write(line)
                        h.update(line)
                        size += len(line)
            result.append({'path': str(PREFIX / target), 'stored_path': str(out.relative_to(ROOT)),
                           'bytes': size, 'stored_bytes': out.stat().st_size,
                           'sha256': h.hexdigest(), 'stored_sha256': sha(out),
                           'records': len(batch), 'source': str(directory), 'cache_kind': kind})
            print(f'Packed {kind} shard {start // 1000}: {out.stat().st_size / 1048576:.2f} MiB', flush=True)
        if kind != 'labels':
            expected = json.loads((source / run / 'summary.json').read_text())['methods'][kind]
            assert len(paths) == expected['completed']
            assert status == {'accepted': expected['accepted'], 'rejected': expected['rejected'], 'infrastructure_error': expected['infrastructure_errors']}
        else:
            assert len(paths) == json.loads((source / run / 'summary.json').read_text())['label_cases']
        print(f'Validated {kind} cache: {len(paths)} rows, {status}', flush=True)
        return result

    jobs = [
        (Path('data/bird_train.jsonl'), None),
        (run / 'prompt/train.jsonl', None), (run / 'mcts/train.jsonl', None),
        (run / 'entity_extract_train.jsonl', None), (run / 'query_rewrite_train.jsonl', None),
        (audit / 'snapshot.json', Path('audit/20261004_232200/snapshot.json')),
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(pack, a, b) for a, b in jobs]
        futures += [pool.submit(pack_cache, k) for k in ['labels', 'prompt', 'mcts']]
        for future in futures:
            entries.extend(future.result())
    entries.sort(key=lambda e: e['path'])
    if any(e['stored_bytes'] >= 50 * 1048576 for e in entries):
        raise ValueError('Compressed artifact too large; use smaller cache shards.')
    manifest_path = ROOT / 'data_manifest.json'
    old = json.loads(manifest_path.read_text())
    old = [e for e in old if not e['path'].startswith(str(PREFIX) + '/')]
    manifest_path.write_text(json.dumps(old + entries, ensure_ascii=False, indent=2) + '\n')
    source_manifest_path = ROOT / 'source_manifest.json'
    original_sources = json.loads(source_manifest_path.read_text())
    original_sources.update(provenance)
    source_manifest_path.write_text(json.dumps(original_sources, ensure_ascii=False, indent=2) + '\n')
    with (ROOT / '.gitignore').open('a') as f:
        for e in entries:
            f.write('/' + e['path'] + '\n')
    (dest / '.gitignore').write_text('.venv*/\n__pycache__/\n*.pyc\nlogs/\nruns/*/cases/\nruns/*/labels/\nruns/vllm_cache/\n')
    print(f'Published {len(entries)} compressed artifacts, {sum(e["stored_bytes"] for e in entries) / 1048576:.2f} MiB', flush=True)

if __name__ == '__main__':
    main()
