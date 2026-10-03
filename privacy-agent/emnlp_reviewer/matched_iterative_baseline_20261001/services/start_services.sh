#!/usr/bin/env bash
RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
set -euo pipefail

SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$SERVICE_DIR/runtime"
LOG_DIR="$SERVICE_DIR/logs"
LMDEPLOY_BIN="${LMDEPLOY_BIN:-/mnt/wh_intern/MXY/envs/miniforge3/envs/privacy-agent/bin/lmdeploy}"
SESSION_LEN="${SESSION_LEN:-32768}"
mkdir -p "$RUNTIME_DIR" "$LOG_DIR"

start_one() {
  local name="$1" gpu="$2" port="$3" tp="$4" model="$5" alias="$6" cache="$7"
  local pid_file="$RUNTIME_DIR/$name.pid" log_file="$LOG_DIR/$name.log"
  python3 - "$port" <<'PY'
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
finally:
    s.close()
PY
  echo "starting $name: GPU=$gpu port=$port tp=$tp model=$model"
  CUDA_VISIBLE_DEVICES="$gpu" nohup "$LMDEPLOY_BIN" serve api_server \
    --server-name 127.0.0.1 \
    --server-port "$port" \
    --backend turbomind \
    --tp "$tp" \
    --session-len "$SESSION_LEN" \
    --cache-max-entry-count "$cache" \
    --model-name "$alias" \
    --log-level INFO \
    "$model" >"$log_file" 2>&1 < /dev/null &
  local pid=$!
  echo "$pid" > "$pid_file"
  sleep 2
  kill -0 "$pid" 2>/dev/null || { echo "$name failed; see $log_file" >&2; exit 1; }
}

REWRITE="${RELEASE_ROOT}/LlamaFactory/saves/query_rewrite_v2/checkpoint-50"
ENTITY="${RELEASE_ROOT}/LlamaFactory/saves/entity_extract_v1/checkpoint-25"
SQL_MODEL="/mnt/wh_intern/MXY/models/Qwen3.8-27B"

start_one rewrite0 0 18436 1 "$REWRITE" query-rewrite-checkpoint-50 0.35
start_one rewrite1 1 18446 1 "$REWRITE" query-rewrite-checkpoint-50 0.35
start_one entity0 2 18437 1 "$ENTITY" entity-extract-checkpoint-25 0.35
start_one entity1 3 18447 1 "$ENTITY" entity-extract-checkpoint-25 0.35
start_one sql0 4,5 18442 2 "$SQL_MODEL" Qwen3.8-27B-local 0.30
start_one sql1 6,7 18452 2 "$SQL_MODEL" Qwen3.8-27B-local 0.30

echo "all six inference services started"
