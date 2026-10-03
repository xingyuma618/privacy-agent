#!/usr/bin/env bash
set -euo pipefail
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$SERVICE_DIR/runtime"
targets=("$@")
[[ ${#targets[@]} -gt 0 ]] || targets=(rewrite entity sft rl)
for name in "${targets[@]}"; do
  pid_file="$RUNTIME_DIR/$name.pid"
  [[ -f "$pid_file" ]] || { echo "$name: no pid file"; continue; }
  pid="$(<"$pid_file")"
  cmdline="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
  if [[ "$cmdline" == *"lmdeploy"* ]] && [[ "$cmdline" == *"checkpoint-50"* || "$cmdline" == *"checkpoint-25"* || "$cmdline" == *"agent_v2_deepseek_pro/checkpoint-40"* || "$cmdline" == *"reward_ablation_1/checkpoint-30"* ]]; then
    kill "$pid"
    echo "$name: sent TERM to pid=$pid"
  elif ! kill -0 "$pid" 2>/dev/null; then
    echo "$name: stale pid=$pid"
  else
    echo "$name: refusing to stop pid=$pid because command does not match this task" >&2
    continue
  fi
  rm -f "$pid_file"
done
