#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
trap 'bash "$HERE/stop_adaptive_agents.sh"' EXIT INT TERM
source "$HERE/start_adaptive_agents.sh"
pids=()
for pid_file in "$HERE"/runtime_adaptive/*.pid; do pids+=("$(<"$pid_file")"); done
wait "${pids[@]}"
