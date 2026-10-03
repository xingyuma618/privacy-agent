#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$HERE/results_300_sharded"

while [[ ! -f "$ROOT/first_150/multi_checkpoint_summary.json" || \
         ! -f "$ROOT/last_150/multi_checkpoint_summary.json" ]]; do
  sleep 30
done

python "$HERE/merge_sharded_results.py" >"$ROOT/merge.log" 2>&1
echo "merged results written to $ROOT/merged_summary.json"
