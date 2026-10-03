#!/usr/bin/env bash
RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$HERE/runtime"
LOG_DIR="$HERE/logs"
LMDEPLOY_BIN="${LMDEPLOY_BIN:-/mnt/wh_intern/MXY/envs/miniforge3/envs/privacy-agent/bin/lmdeploy}"
mkdir -p "$RUNTIME_DIR" "$LOG_DIR"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

start_one() {
  local name="$1" gpu="$2" port="$3" tp="$4" model="$5" alias="$6" cache="$7"
  local pid_file="$RUNTIME_DIR/$name.pid" log_file="$LOG_DIR/$name.log"
  if [[ -f "$pid_file" ]] && kill -0 "$(<"$pid_file")" 2>/dev/null; then
    echo "$name already running (pid=$(<"$pid_file"))"
    return 0
  fi
  python3 - "$port" <<'PY'
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
finally:
    s.close()
PY
  CUDA_VISIBLE_DEVICES="$gpu" nohup "$LMDEPLOY_BIN" serve api_server \
    --server-name 127.0.0.1 \
    --server-port "$port" \
    --backend turbomind \
    --tp "$tp" \
    --session-len 32768 \
    --cache-max-entry-count "$cache" \
    --model-name "$alias" \
    --log-level INFO \
    "$model" >"$log_file" 2>&1 < /dev/null &
  local pid=$!
  PIDS+=("$pid")
  echo "$pid" > "$pid_file"
  sleep 2
  kill -0 "$pid" 2>/dev/null || { echo "$name failed; see $log_file" >&2; exit 1; }
  echo "$name pid=$pid port=$port"
}

start_one rewrite 2 18536 1 \
  ${RELEASE_ROOT}/LlamaFactory/saves/query_rewrite_v2/checkpoint-50 \
  query-rewrite-checkpoint-50 0.35
start_one entity 3 18537 1 \
  ${RELEASE_ROOT}/LlamaFactory/saves/entity_extract_v1/checkpoint-25 \
  entity-extract-checkpoint-25 0.35
start_one sql 4,5 18542 2 \
  /mnt/wh_intern/MXY/models/Qwen3.8-27B \
  Qwen3.8-27B-local 0.30

echo "auxiliary services started"

if [[ "${HOLD_SERVICES:-0}" == "1" ]]; then
  wait
fi
