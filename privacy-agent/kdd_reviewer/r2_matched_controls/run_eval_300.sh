#!/usr/bin/env bash
set -euo pipefail

ROOT=/mnt/wh_intern/MXY/paper
PROJECT="$ROOT/privacy-agent"
PYTHON="$ROOT/../envs/miniforge3/envs/privacy-agent/bin/python"
LMDEPLOY="$ROOT/../envs/miniforge3/envs/privacy-agent/bin/lmdeploy"

cd "$PROJECT"

# The three auxiliary services must already be listening on ports 18536,
# 18537, and 18542. Both controller checkpoints run concurrently, one per GPU,
# and see exactly the same 300 MaskSQL/BIRD cases and inference limits.
export QUERY_REWRITE_API_KEY=EMPTY
export QUERY_REWRITE_BASE_URL=http://127.0.0.1:18536/v1
export QUERY_REWRITE_MODEL_NAME=query-rewrite-checkpoint-50
export ENTITY_EXTRACT_API_KEY=EMPTY
export ENTITY_EXTRACT_BASE_URL=http://127.0.0.1:18537/v1
export ENTITY_EXTRACT_MODEL_NAME=entity-extract-checkpoint-25
export SQL_GENERATE_API_KEY=EMPTY
export SQL_GENERATE_BASE_URL=http://127.0.0.1:18542/v1
export SQL_GENERATE_MODEL_NAME=Qwen3.8-27B-local

env -u ALL_PROXY -u all_proxy \
    -u HTTPS_PROXY -u https_proxy \
    -u HTTP_PROXY -u http_proxy \
    "$PYTHON" agent_data_create/ineference/agent_eval_multi_checkpoint_lmdeploy_token_usage_privacy_f1.py \
    --checkpoints \
      "$ROOT/LlamaFactory/saves/r2_prompt_synthesis_n3101" \
      "$ROOT/LlamaFactory/saves/r2_mcts_synthesis_n3101" \
    --gpus 0,1 \
    --max_parallel 2 \
    --base_port 18540 \
    --server_cmd_template "$LMDEPLOY serve api_server --log-level=INFO --server-port {port} --tp=1 {model_path} {extra_args}" \
    --server_extra_args "--session-len 32768 --cache-max-entry-count 0.35" \
    --per_checkpoint_workers 8 \
    --output_dir "$PROJECT/kdd_reviewer/r2_matched_controls/results_300_run1_stable" \
    --disable_pretransmission_gate \
    --db_base_dir "$PROJECT/CHESS/data/dev/dev_databases" \
    --ground_truth_path "$ROOT/rl/ours/CHESS/data/dev/dev.sql"
