#!/usr/bin/env python3
"""Verify the release artifacts and restore compressed datasets without dependencies."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-only', action='store_true', help='Validate stored files without extracting them.')
    args = parser.parse_args()
    entries = json.loads((ROOT / 'data_manifest.json').read_text())
    for entry in entries:
        stored = ROOT / entry['stored_path']
        target = ROOT / entry['path']
        if digest(stored) != entry['stored_sha256']:
            raise SystemExit(f'Checksum mismatch: {stored}')
        if args.check_only or stored == target:
            continue
        if target.exists():
            if digest(target) != entry['sha256']:
                raise SystemExit(f'Existing data differs; move it aside before extracting: {target}')
            continue
        temporary = target.with_name(target.name + '.extracting')
        h = hashlib.sha256()
        try:
            with gzip.open(stored, 'rb') as source, temporary.open('wb') as dest:
                for chunk in iter(lambda: source.read(1024 * 1024), b''):
                    h.update(chunk)
                    dest.write(chunk)
            if h.hexdigest() != entry['sha256']:
                raise SystemExit(f'Extracted checksum mismatch: {stored}')
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        print(f'Restored {entry["path"]}')
    print(f'Validated {len(entries)} data files.')

if __name__ == '__main__':
    main()
