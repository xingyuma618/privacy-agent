#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
expected_manifest="9d4559aba9ddf006bc7682d49ef862129ce6850dbd678078c7d8f052d1d969b2"
expected_runner="4ef6da872827e33f9e2b7cecd86608b82c19896963aec0dc50cf405d28e9d785"
test "$(sha256sum manifest.json | awk '{print $1}')" = "$expected_manifest"
test "$(sha256sum runner.py | awk '{print $1}')" = "$expected_runner"
test "$(ps -p 225598 -o stat=)" = "Ts+"
test "$(ps -p 225598 -o cmd=)" = "bash ./reviewer/sft_vs_rl_20261001/run_recovery_sequence.sh"
test "$(ps -p 225678 -o ppid= | tr -d ' ')" = "225598"

for path in \
  results_local_qwen/sft/run-2 results_local_qwen/rl/run-2 \
  results_local_qwen/sft/run-3 results_local_qwen/rl/run-3; do
  test ! -e "$path"
done

python3 run_replica_eval.py sft 2 --output results_local_qwen/sft/run-2 --workers 16 --execute-reviewed &
sft2_pid=$!
python3 run_replica_eval.py rl 2 --output results_local_qwen/rl/run-2 --workers 16 --execute-reviewed &
rl2_pid=$!
python3 runner.py rl 3 --output results_local_qwen/rl/run-3 --workers 16 --execute-reviewed &
rl3_pid=$!

while kill -0 225678 2>/dev/null; do
  sleep 5
done
python3 - <<'PY'
import json
from pathlib import Path
p = Path("results_local_qwen/sft/run-1")
m = json.loads((p / "metadata.json").read_text())
assert m["status"] == "complete" and m["completed_cases"] == 300
assert len((p / "cases.jsonl").read_text().splitlines()) == 300
PY

# SIGTERM remains pending for a stopped process; SIGCONT lets it terminate
# without executing the old controller's next evaluation command.
kill -TERM 225598
kill -CONT 225598
for unused in $(seq 1 20); do
  kill -0 225598 2>/dev/null || break
  sleep 0.25
done
if kill -0 225598 2>/dev/null; then
  echo "old controller did not terminate" >&2
  exit 1
fi

python3 runner.py sft 3 --output results_local_qwen/sft/run-3 --workers 16 --execute-reviewed &
sft3_pid=$!

status=0
wait "$sft2_pid" || status=1
wait "$rl2_pid" || status=1
wait "$rl3_pid" || status=1
wait "$sft3_pid" || status=1
test "$status" = 0

python3 - <<'PY'
import json
from pathlib import Path
root = Path("results_local_qwen")
for arm in ("sft", "rl"):
    for repetition, workers in ((1, 8), (2, 16), (3, 16)):
        p = root / arm / f"run-{repetition}"
        m = json.loads((p / "metadata.json").read_text())
        rows = [json.loads(x) for x in (p / "cases.jsonl").read_text().splitlines()]
        assert m["status"] == "complete" and m["completed_cases"] == 300
        assert m["workers"] == workers and len(rows) == 300
        assert all(row["task_status"] == "valid" for row in rows)
        assert m["manifest_sha256"] == "9d4559aba9ddf006bc7682d49ef862129ce6850dbd678078c7d8f052d1d969b2"
        assert m["runner_sha256"] == "4ef6da872827e33f9e2b7cecd86608b82c19896963aec0dc50cf405d28e9d785"
print("REPLICA_PARALLEL_SEQUENCE_COMPLETE")
PY
