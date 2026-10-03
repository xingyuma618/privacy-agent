#!/usr/bin/env bash
RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
set -euo pipefail
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$SERVICE_DIR/runtime_adaptive"
LOG_DIR="$SERVICE_DIR/logs_adaptive"
LMDEPLOY_BIN="${LMDEPLOY_BIN:-/mnt/wh_intern/MXY/envs/miniforge3/envs/privacy-agent/bin/lmdeploy}"
MODEL="${RELEASE_ROOT}/LlamaFactory/saves/agent_v2_deepseek_pro/checkpoint-40"
mkdir -p "$RUNTIME_DIR" "$LOG_DIR"

start_one() {
  local name="$1" gpu="$2" port="$3"
  python3 - "$port" <<'PY'
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
finally:
    s.close()
PY
  CUDA_VISIBLE_DEVICES="$gpu" nohup "$LMDEPLOY_BIN" serve api_server \
    --server-name 127.0.0.1 --server-port "$port" --backend turbomind --tp 1 \
    --session-len 32768 --cache-max-entry-count 0.20 \
    --model-name PrivAgent-R1-SFT-checkpoint-40 --log-level INFO \
    "$MODEL" >"$LOG_DIR/$name.log" 2>&1 < /dev/null &
  echo "$!" >"$RUNTIME_DIR/$name.pid"
  sleep 2
  kill -0 "$(<"$RUNTIME_DIR/$name.pid")" 2>/dev/null || {
    echo "$name failed; see $LOG_DIR/$name.log" >&2; exit 1;
  }
}

start_one agent0 0 18443
start_one agent1 1 18453
start_one agent2 2 18441
start_one agent3 3 18451
echo "all four adaptive-agent replicas started"
