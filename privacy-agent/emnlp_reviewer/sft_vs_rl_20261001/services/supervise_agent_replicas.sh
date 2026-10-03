#!/usr/bin/env bash
set -euo pipefail
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cleanup() {
  for name in sft_replica rl_replica; do
    pid_file="$SERVICE_DIR/runtime/$name.pid"
    if [[ -f "$pid_file" ]] && kill -0 "$(<"$pid_file")" 2>/dev/null; then
      kill "$(<"$pid_file")" 2>/dev/null || true
      wait "$(<"$pid_file")" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

source "$SERVICE_DIR/start_agent_replicas.sh"
pids=()
for name in sft_replica rl_replica; do pids+=("$(<"$SERVICE_DIR/runtime/$name.pid")"); done
echo "replica supervisor holding service pids: ${pids[*]}"
wait -n "${pids[@]}"
echo "a replica exited; shutting down the remaining replica" >&2
exit 1
