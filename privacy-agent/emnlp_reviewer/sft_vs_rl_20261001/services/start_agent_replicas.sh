#!/usr/bin/env bash
RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
set -euo pipefail

SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$SERVICE_DIR/runtime"
LOG_DIR="$SERVICE_DIR/logs"
LMDEPLOY_BIN="${LMDEPLOY_BIN:-/mnt/wh_intern/MXY/envs/miniforge3/envs/privacy-agent/bin/lmdeploy}"
SESSION_LEN="${SESSION_LEN:-32768}"
CACHE_FRACTION="${CACHE_FRACTION:-0.35}"
mkdir -p "$RUNTIME_DIR" "$LOG_DIR"

declare -A GPU PORT MODEL MODEL_NAME
GPU[sft_replica]=6
PORT[sft_replica]=18346
MODEL[sft_replica]="${RELEASE_ROOT}/LlamaFactory/saves/agent_v2_deepseek_pro/checkpoint-40"
MODEL_NAME[sft_replica]="PrivAgent-R1-SFT-checkpoint-40"

GPU[rl_replica]=7
PORT[rl_replica]=18347
MODEL[rl_replica]="${RELEASE_ROOT}/privacy-agent/verl/output/models/qwen/Qwen3-4B-Instruct/reward_ablation_1/checkpoint-30"
MODEL_NAME[rl_replica]="PrivAgent-R1-RL-step10"

port_free() {
  python3 - "${1}" <<'PY'
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
finally:
    s.close()
PY
}

start_one() {
  local name="$1" pid_file="$RUNTIME_DIR/$1.pid" log_file="$LOG_DIR/$1.log"
  if [[ -f "$pid_file" ]] && kill -0 "$(<"$pid_file")" 2>/dev/null; then
    echo "$name already running (pid=$(<"$pid_file"))"
    return 0
  fi
  [[ -x "$LMDEPLOY_BIN" ]] || { echo "lmdeploy is not executable: $LMDEPLOY_BIN" >&2; return 1; }
  [[ -f "${MODEL[$name]}/config.json" ]] || { echo "model missing: ${MODEL[$name]}" >&2; return 1; }
  port_free "${PORT[$name]}" || { echo "port ${PORT[$name]} is already occupied" >&2; return 1; }

  echo "starting $name: GPU=${GPU[$name]} port=${PORT[$name]} model=${MODEL[$name]}"
  CUDA_VISIBLE_DEVICES="${GPU[$name]}" nohup "$LMDEPLOY_BIN" serve api_server \
    --server-name 127.0.0.1 \
    --server-port "${PORT[$name]}" \
    --tp 1 \
    --session-len "$SESSION_LEN" \
    --cache-max-entry-count "$CACHE_FRACTION" \
    --model-name "${MODEL_NAME[$name]}" \
    --log-level INFO \
    "${MODEL[$name]}" >"$log_file" 2>&1 < /dev/null &
  local pid=$!
  echo "$pid" > "$pid_file"
  sleep 2
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "$name failed during startup; see $log_file" >&2
    return 1
  fi
  echo "$name pid=$pid base_url=http://127.0.0.1:${PORT[$name]}/v1 model=${MODEL_NAME[$name]}"
}

start_one sft_replica
start_one rl_replica
