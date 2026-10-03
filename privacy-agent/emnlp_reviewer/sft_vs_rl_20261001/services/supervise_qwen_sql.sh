#!/usr/bin/env bash
set -euo pipefail
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cleanup() {
  if [[ -n "${QWEN_SQL_PID:-}" ]] && kill -0 "$QWEN_SQL_PID" 2>/dev/null; then
    kill "$QWEN_SQL_PID" 2>/dev/null || true
    wait "$QWEN_SQL_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# Source so the server is a direct child and remains attached to this
# foreground supervisor for the lifetime of the evaluation.
source "$SERVICE_DIR/start_qwen_sql.sh"
wait "$QWEN_SQL_PID"
