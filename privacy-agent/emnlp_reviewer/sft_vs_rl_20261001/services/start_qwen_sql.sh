#!/usr/bin/env bash
set -euo pipefail

SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$SERVICE_DIR/runtime"
LOG_DIR="$SERVICE_DIR/logs"
LMDEPLOY_BIN="${LMDEPLOY_BIN:-/mnt/wh_intern/MXY/envs/miniforge3/envs/privacy-agent/bin/lmdeploy}"
MODEL_PATH="/mnt/wh_intern/MXY/models/Qwen3.8-27B"
MODEL_ID="Qwen3.8-27B-local"
PORT=18342
mkdir -p "$RUNTIME_DIR" "$LOG_DIR"

python3 - "$PORT" <<'PY'
import socket, sys
s = socket.socket()
try:
    s.bind(('127.0.0.1', int(sys.argv[1])))
finally:
    s.close()
PY

echo "starting qwen_sql: GPUs=4,5 port=$PORT backend=turbomind tp=2 model=$MODEL_PATH"
CUDA_VISIBLE_DEVICES=4,5 "$LMDEPLOY_BIN" serve api_server \
  --server-name 127.0.0.1 \
  --server-port "$PORT" \
  --backend turbomind \
  --tp 2 \
  --session-len 32768 \
  --cache-max-entry-count 0.30 \
  --model-name "$MODEL_ID" \
  --log-level INFO \
  "$MODEL_PATH" >"$LOG_DIR/qwen_sql.log" 2>&1 < /dev/null &
QWEN_SQL_PID=$!
echo "$QWEN_SQL_PID" > "$RUNTIME_DIR/qwen_sql.pid"
echo "qwen_sql pid=$QWEN_SQL_PID base_url=http://127.0.0.1:$PORT/v1 model=$MODEL_ID"

