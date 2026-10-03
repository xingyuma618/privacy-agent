#!/usr/bin/env bash
set -euo pipefail
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$SERVICE_DIR/runtime"
for pid_file in "$RUNTIME_DIR"/*.pid; do
  [[ -e "$pid_file" ]] || continue
  pid="$(<"$pid_file")"
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
  fi
done
