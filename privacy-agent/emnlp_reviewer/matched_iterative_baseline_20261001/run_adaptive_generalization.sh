#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-/mnt/wh_intern/MXY/envs/miniforge3/envs/privacy-agent/bin/python}"
for dataset in spider_dev spider_test; do
  "$PYTHON" "$HERE/adaptive_generalization_runner.py" \
    --dataset "$dataset" --limit 300 --workers "${WORKERS:-16}" \
    --output "$HERE/results_adaptive_generalization_4rep/$dataset/sft/run-1"
done
"$PYTHON" "$HERE/summarize_cross_dataset.py" \
  --output "$HERE/cross_dataset_summary.json"
