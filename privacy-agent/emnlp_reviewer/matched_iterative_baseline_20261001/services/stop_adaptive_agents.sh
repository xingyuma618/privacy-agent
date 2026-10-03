#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
shopt -s nullglob
for pid_file in "$HERE"/runtime_adaptive/*.pid; do
  pid="$(<"$pid_file")"
  kill "$pid" 2>/dev/null || true
done
for pid_file in "$HERE"/runtime_adaptive/*.pid; do
  pid="$(<"$pid_file")"
  for _ in {1..20}; do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.25
  done
done
