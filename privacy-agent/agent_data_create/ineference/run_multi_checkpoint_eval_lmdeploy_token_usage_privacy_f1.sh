#!/usr/bin/env bash
set -euo pipefail

# =========================
# LMDeploy 多 checkpoint 并行评估 + agent pipeline token usage 统计
# =========================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DATA_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PRIVACY_AGENT_ROOT="$(cd "$AGENT_DATA_DIR/.." && pwd)"

EXP_NAME="${EXP_NAME:-qwen3-4b_function_rm-gsm8k-sgl-multi-w-tool-verify-n16}"

CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-$PRIVACY_AGENT_ROOT/verl/output/models/qwen/Qwen3-4B-Instruct/${EXP_NAME}}"
OUTPUT_DIR="${OUTPUT_DIR:-$SCRIPT_DIR/output_multi_ckpt_token_usage_privacy_f1/${EXP_NAME}}"

# SQL 生成 API；RI attack 默认也复用这套配置
export SQL_GENERATE_API_KEY="${SQL_GENERATE_API_KEY:-EMPTY}"
export SQL_GENERATE_BASE_URL="${SQL_GENERATE_BASE_URL:-http://127.0.0.1:8039/v1}"
export SQL_GENERATE_MODEL_NAME="${SQL_GENERATE_MODEL_NAME:-Qwen3.6-27B}"

RUN_RI_ATTACK="${RUN_RI_ATTACK:-0}"

RI_FLAG=""
if [[ "$RUN_RI_ATTACK" == "1" ]]; then
  RI_FLAG="--run_ri_attack"
fi

python "$SCRIPT_DIR/agent_eval_multi_checkpoint_lmdeploy_token_usage_privacy_f1.py" \
  --checkpoint_root "$CHECKPOINT_ROOT" \
  --checkpoint_pattern "checkpoint-*" \
  --gpus "${GPUS:-0,1,2,3,4,5,6,7}" \
  --max_parallel "${MAX_PARALLEL:-8}" \
  --base_port "${BASE_PORT:-2337}" \
  --server_backend lmdeploy \
  --server_extra_args "${SERVER_EXTRA_ARGS:---max-batch-size 32}" \
  --per_checkpoint_workers "${PER_CHECKPOINT_WORKERS:-32}" \
  --output_dir "$OUTPUT_DIR" \
  $RI_FLAG
