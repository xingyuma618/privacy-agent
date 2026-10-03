#!/usr/bin/env bash
set -euo pipefail
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cleanup() {
  bash "$SERVICE_DIR/stop_services.sh" rewrite entity sft rl || true
}
trap cleanup EXIT INT TERM

# The execution platform reaps detached children once their launching command exits.
# Keep this supervisor in the foreground for the entire evaluation lifetime.
# Source the launcher so the model servers are direct children of this
# long-lived supervisor.  Launching it via `bash` makes them grandchildren;
# Bash cannot `wait` for those recorded PIDs and immediately runs cleanup.
source "$SERVICE_DIR/start_services.sh" rewrite entity sft rl
pids=()
for name in rewrite entity sft rl; do pids+=("$(<"$SERVICE_DIR/runtime/$name.pid")"); done
echo "supervisor holding service pids: ${pids[*]}"
wait -n "${pids[@]}"
echo "a service exited; shutting down the remaining services" >&2
exit 1
