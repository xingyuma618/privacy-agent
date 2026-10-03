#!/usr/bin/env bash
set -euo pipefail
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for spec in rewrite:18336 entity:18337 sft:18340 rl:18341; do
  name="${spec%%:*}"; port="${spec##*:}"; pid_file="$SERVICE_DIR/runtime/$name.pid"
  pid="-"; state="stopped"
  if [[ -f "$pid_file" ]]; then pid="$(<"$pid_file")"; kill -0 "$pid" 2>/dev/null && state="running" || state="stale"; fi
  printf '%-8s pid=%-8s state=%-8s url=http://127.0.0.1:%s/v1\n' "$name" "$pid" "$state" "$port"
done
