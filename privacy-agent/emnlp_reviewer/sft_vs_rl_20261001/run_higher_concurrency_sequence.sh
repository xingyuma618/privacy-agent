#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

expected_manifest="9d4559aba9ddf006bc7682d49ef862129ce6850dbd678078c7d8f052d1d969b2"
expected_runner="4ef6da872827e33f9e2b7cecd86608b82c19896963aec0dc50cf405d28e9d785"
test "$(sha256sum manifest.json | awk '{print $1}')" = "$expected_manifest"
test "$(sha256sum runner.py | awk '{print $1}')" = "$expected_runner"

python3 - <<'PY'
import json
from pathlib import Path

metadata = json.loads(Path("results_local_qwen/sft/run-1/metadata.json").read_text())
assert metadata["status"] == "complete"
assert metadata["completed_cases"] == 300
assert len(Path("results_local_qwen/sft/run-1/cases.jsonl").read_text().splitlines()) == 300
PY

for path in \
  results_local_qwen/sft/run-2 results_local_qwen/rl/run-2 \
  results_local_qwen/sft/run-3 results_local_qwen/rl/run-3; do
  test ! -e "$path"
done

python3 runner.py sft 2 --output results_local_qwen/sft/run-2 --workers 16 --execute-reviewed &
sft2_pid=$!
python3 runner.py rl 2 --output results_local_qwen/rl/run-2 --workers 16 --execute-reviewed &
rl2_pid=$!
wait "$sft2_pid"
wait "$rl2_pid"

python3 runner.py sft 3 --output results_local_qwen/sft/run-3 --workers 16 --execute-reviewed &
sft3_pid=$!
python3 runner.py rl 3 --output results_local_qwen/rl/run-3 --workers 16 --execute-reviewed &
rl3_pid=$!
wait "$sft3_pid"
wait "$rl3_pid"

python3 - <<'PY'
import json
from pathlib import Path

root = Path("results_local_qwen")
for arm in ("sft", "rl"):
    for repetition, expected_workers in ((1, 8), (2, 16), (3, 16)):
        directory = root / arm / f"run-{repetition}"
        metadata = json.loads((directory / "metadata.json").read_text())
        rows = [json.loads(line) for line in (directory / "cases.jsonl").read_text().splitlines()]
        assert metadata["status"] == "complete"
        assert metadata["completed_cases"] == 300
        assert metadata["workers"] == expected_workers
        assert len(rows) == 300
        assert all(row["task_status"] == "valid" for row in rows)
        assert metadata["manifest_sha256"] == "9d4559aba9ddf006bc7682d49ef862129ce6850dbd678078c7d8f052d1d969b2"
        assert metadata["runner_sha256"] == "4ef6da872827e33f9e2b7cecd86608b82c19896963aec0dc50cf405d28e9d785"
print("HIGHER_CONCURRENCY_SEQUENCE_COMPLETE")
PY
