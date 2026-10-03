#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-/mnt/wh_intern/MXY/envs/miniforge3/envs/privacy-agent/bin/python}"
RESULTS="$HERE/results"
WORKERS="${WORKERS:-24}"

for run in 1 2 3; do
  "$PYTHON" "$HERE/runner.py" one_pass_fixed "$run" \
    --output "$RESULTS/one_pass_fixed/run-$run" --workers "$WORKERS"
  "$PYTHON" "$HERE/runner.py" iterative_fixed "$run" \
    --output "$RESULTS/iterative_fixed/run-$run" --workers "$WORKERS"
done

"$PYTHON" "$HERE/summarize.py" "$RESULTS" --output "$HERE/summary.json"
