#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

test "$(sha256sum manifest.json | awk '{print $1}')" = "9d4559aba9ddf006bc7682d49ef862129ce6850dbd678078c7d8f052d1d969b2"
test "$(sha256sum runner.py | awk '{print $1}')" = "4ef6da872827e33f9e2b7cecd86608b82c19896963aec0dc50cf405d28e9d785"
test "$(ps -p 232380 -o stat=)" = "Ts+"
python3 - <<'PY'
import json
from pathlib import Path
p = Path("results_local_qwen/sft/run-1")
m = json.loads((p / "metadata.json").read_text())
assert m["status"] == "complete" and m["completed_cases"] == 300
assert len((p / "cases.jsonl").read_text().splitlines()) == 300
PY
test ! -e results_local_qwen/sft/run-3

python3 runner.py sft 3 --output results_local_qwen/sft/run-3 --workers 16 --execute-reviewed &
sft3_pid=$!

while ! python3 - <<'PY'
import json
from pathlib import Path
for arm, run in (("sft", 2), ("rl", 2), ("rl", 3), ("sft", 3)):
    p = Path("results_local_qwen") / arm / f"run-{run}"
    if not (p / "metadata.json").is_file(): raise SystemExit(1)
    m = json.loads((p / "metadata.json").read_text())
    if m.get("status") != "complete" or m.get("completed_cases") != 300: raise SystemExit(1)
    if len((p / "cases.jsonl").read_text().splitlines()) != 300: raise SystemExit(1)
PY
do
  for pid in 232394 232395 232396 "$sft3_pid"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "runner $pid exited before complete metadata" >&2
      exit 1
    fi
  done
  sleep 5
done
wait "$sft3_pid"

kill -TERM 232380
kill -CONT 232380
for unused in $(seq 1 20); do
  kill -0 232380 2>/dev/null || break
  sleep 0.25
done
! kill -0 232380 2>/dev/null

python3 - <<'PY'
import json
from pathlib import Path
root = Path("results_local_qwen")
for arm in ("sft", "rl"):
    for run, workers in ((1, 8), (2, 16), (3, 16)):
        p = root / arm / f"run-{run}"
        m = json.loads((p / "metadata.json").read_text())
        rows = [json.loads(x) for x in (p / "cases.jsonl").read_text().splitlines()]
        assert m["status"] == "complete" and m["completed_cases"] == 300
        assert m["workers"] == workers and len(rows) == 300
        assert all(row["task_status"] == "valid" for row in rows)
print("CONTINUATION_COMPLETE")
PY
