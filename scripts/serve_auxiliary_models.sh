#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

QUERY_REWRITE_MODEL="${QUERY_REWRITE_MODEL:-$ROOT/LlamaFactory/saves/query_rewrite_v2/checkpoint-50}"
ENTITY_EXTRACT_MODEL="${ENTITY_EXTRACT_MODEL:-$ROOT/LlamaFactory/saves/entity_extract_v1/checkpoint-25}"
QUERY_REWRITE_PORT="${QUERY_REWRITE_PORT:-8036}"
ENTITY_EXTRACT_PORT="${ENTITY_EXTRACT_PORT:-8037}"

if [[ ! -d "$QUERY_REWRITE_MODEL" ]]; then
  echo "Query rewrite model not found: $QUERY_REWRITE_MODEL"
  echo "Train it with: bash scripts/train_sft.sh query"
  exit 1
fi

if [[ ! -d "$ENTITY_EXTRACT_MODEL" ]]; then
  echo "Entity extraction model not found: $ENTITY_EXTRACT_MODEL"
  echo "Train it with: bash scripts/train_sft.sh entity"
  exit 1
fi

lmdeploy serve api_server --model-name "${QUERY_REWRITE_MODEL_NAME:-query_rewrite_v2}" --log-level=INFO --server-port "$QUERY_REWRITE_PORT" --tp="${QUERY_REWRITE_TP:-1}" "$QUERY_REWRITE_MODEL" &
QUERY_PID=$!

lmdeploy serve api_server --model-name "${ENTITY_EXTRACT_MODEL_NAME:-entity_extract_v1}" --log-level=INFO --server-port "$ENTITY_EXTRACT_PORT" --tp="${ENTITY_EXTRACT_TP:-1}" "$ENTITY_EXTRACT_MODEL" &
ENTITY_PID=$!

echo "Query rewrite server pid=$QUERY_PID port=$QUERY_REWRITE_PORT"
echo "Entity extraction server pid=$ENTITY_PID port=$ENTITY_EXTRACT_PORT"
wait
