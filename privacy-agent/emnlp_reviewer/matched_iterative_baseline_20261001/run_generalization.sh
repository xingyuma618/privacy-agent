#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-/mnt/wh_intern/MXY/envs/miniforge3/envs/privacy-agent/bin/python}"
RESULTS="$HERE/results_generalization"
WORKERS="${WORKERS:-24}"

for dataset in bird_dev_full spider_dev spider_test; do
  for variant in one_pass_fixed iterative_fixed; do
    "$PYTHON" "$HERE/runner.py" "$variant" 1 \
      --dataset "$dataset" \
      --output "$RESULTS/$dataset/$variant/run-1" \
      --workers "$WORKERS"
  done
done

"$PYTHON" "$HERE/summarize_generalization.py" "$RESULTS" \
  --output "$HERE/generalization_summary.json"
