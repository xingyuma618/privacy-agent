#!/usr/bin/env bash
set -euo pipefail

ROOT=/mnt/wh_intern/MXY/paper
PROJECT="$ROOT/privacy-agent"
PYTHON="$ROOT/../envs/miniforge3/envs/privacy-agent/bin/python"
LMDEPLOY="$ROOT/../envs/miniforge3/envs/privacy-agent/bin/lmdeploy"
EVALUATOR=agent_data_create/ineference/agent_eval_multi_checkpoint_lmdeploy_token_usage_privacy_f1.py
OUT="$PROJECT/kdd_reviewer/r2_matched_controls/results_300_sharded"

cd "$PROJECT"
export QUERY_REWRITE_API_KEY=EMPTY
export QUERY_REWRITE_BASE_URL=http://127.0.0.1:18536/v1
export QUERY_REWRITE_MODEL_NAME=query-rewrite-checkpoint-50
export ENTITY_EXTRACT_API_KEY=EMPTY
export ENTITY_EXTRACT_BASE_URL=http://127.0.0.1:18537/v1
export ENTITY_EXTRACT_MODEL_NAME=entity-extract-checkpoint-25
export SQL_GENERATE_API_KEY=EMPTY
export SQL_GENERATE_BASE_URL=http://127.0.0.1:18542/v1
export SQL_GENERATE_MODEL_NAME=Qwen3.8-27B-local

run_shard() {
  local name="$1" offset="$2" gpus="$3" port="$4"
  local shard_out="$OUT/$name"
  mkdir -p "$shard_out"
  env -u ALL_PROXY -u all_proxy \
      -u HTTPS_PROXY -u https_proxy \
      -u HTTP_PROXY -u http_proxy \
      "$PYTHON" "$EVALUATOR" \
      --checkpoints \
        "$ROOT/LlamaFactory/saves/r2_prompt_synthesis_n3101" \
        "$ROOT/LlamaFactory/saves/r2_mcts_synthesis_n3101" \
      --gpus "$gpus" \
      --max_parallel 2 \
      --base_port "$port" \
      --server_cmd_template "$LMDEPLOY serve api_server --log-level=INFO --server-port {port} --tp=1 {model_path} {extra_args}" \
      --server_extra_args "--session-len 32768 --cache-max-entry-count 0.35" \
      --per_checkpoint_workers 8 \
      --eval_sample_offset "$offset" \
      --max_eval_samples 150 \
      --output_dir "$shard_out" \
      --disable_pretransmission_gate \
      --db_base_dir "$PROJECT/CHESS/data/dev/dev_databases" \
      --ground_truth_path "$ROOT/rl/ours/CHESS/data/dev/dev.sql" \
      >"$shard_out/runner.log" 2>&1
}

run_shard first_150 0 0,1 18540 &
pid_first=$!
run_shard last_150 150 6,7 18544 &
pid_last=$!
trap 'kill "$pid_first" "$pid_last" 2>/dev/null || true' INT TERM EXIT

wait "$pid_first"
wait "$pid_last"
trap - INT TERM EXIT
echo "both 150-question shards completed"
