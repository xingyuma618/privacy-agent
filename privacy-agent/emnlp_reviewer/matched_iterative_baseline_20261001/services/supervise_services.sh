#!/usr/bin/env bash
set -euo pipefail
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
trap 'bash "$SERVICE_DIR/stop_services.sh"' EXIT INT TERM
source "$SERVICE_DIR/start_services.sh"
pids=()
for pid_file in "$SERVICE_DIR"/runtime/*.pid; do
  pids+=("$(<"$pid_file")")
done
wait "${pids[@]}"
