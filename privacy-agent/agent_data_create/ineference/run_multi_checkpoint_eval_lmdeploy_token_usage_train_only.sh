#!/usr/bin/env bash
RELEASE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
set -euo pipefail

# =========================
# LMDeploy 多 checkpoint 并行评估 + agent pipeline token usage 统计
# =========================

export RUN_RI_ATTACK=1
export SQL_GENERATE_API_KEY="${SQL_GENERATE_API_KEY:-EMPTY}"
export CHECKPOINT_ROOT=${RELEASE_ROOT}/privacy-agent/verl/output/models/qwen/Qwen3-4B-Instruct/reward_ablation
export EXP_NAME=reward_ablation

EXP_NAME="${EXP_NAME:-qwen3-4b_function_rm-gsm8k-sgl-multi-w-tool-verify-n16_1}"

CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-${RELEASE_ROOT}/privacy-agent/verl/output/models/qwen/Qwen3-4B-Instruct/${EXP_NAME}}"
OUTPUT_DIR="${OUTPUT_DIR:-${RELEASE_ROOT}/privacy-agent/agent_data_create/ineference/output_multi_ckpt_token_usage_train_only/${EXP_NAME}}"

# SQL 生成 API；RI attack 默认也复用这套配置
export SQL_GENERATE_API_KEY="${SQL_GENERATE_API_KEY:-EMPTY}"
export SQL_GENERATE_BASE_URL="${SQL_GENERATE_BASE_URL:-https://api.deepseek.com/v1}"
export SQL_GENERATE_MODEL_NAME="${SQL_GENERATE_MODEL_NAME:-deepseek-v4-flash}"

RUN_RI_ATTACK="${RUN_RI_ATTACK:-0}"

RI_FLAG=""
if [[ "$RUN_RI_ATTACK" == "1" ]]; then
  RI_FLAG="--run_ri_attack"
fi

python agent_eval_multi_checkpoint_lmdeploy_token_usage_train_only.py \
  --checkpoint_root "$CHECKPOINT_ROOT" \
  --checkpoint_pattern "checkpoint-*" \
  --gpus "${GPUS:-0,1,2,3,4,5,6,7}" \
  --max_parallel "${MAX_PARALLEL:-8}" \
  --base_port "${BASE_PORT:-2337}" \
  --server_backend lmdeploy \
  --server_extra_args "${SERVER_EXTRA_ARGS:---max-batch-size 8}" \
  --per_checkpoint_workers "${PER_CHECKPOINT_WORKERS:-8}" \
  --output_dir "$OUTPUT_DIR" \
  $RI_FLAG
