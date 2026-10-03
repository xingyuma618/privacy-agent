#!/usr/bin/env bash
# Run from any directory. The script resolves paths relative to this zip package.

set -euo pipefail

# ulimit -n 65535

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PACKAGE_ROOT="$(cd "$PROJECT_DIR/../.." && pwd)"
LLAMAFACTORY_ROOT="$PACKAGE_ROOT/LlamaFactory"
CONFIG_PATH="$PROJECT_DIR/examples/sglang_multiturn/config"

MODEL_PATH="${MODEL_PATH:-$LLAMAFACTORY_ROOT/saves/agent_v2_mcts/checkpoint-40}"
TRAIN_FILE="${TRAIN_FILE:-$PROJECT_DIR/data/privacy-agent/train.parquet}"
VAL_FILE="${VAL_FILE:-$PROJECT_DIR/data/privacy-agent/test.parquet}"
TOOL_CONFIG_PATH="${TOOL_CONFIG_PATH:-$PROJECT_DIR/examples/sglang_multiturn/config/tool_config/privacy_tools.yaml}"

export WANDB_API_KEY="${WANDB_API_KEY:-}"
export WANDB_MODE="${WANDB_MODE:-offline}"

CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1,2,3}" python -m verl.trainer.main_ppo \
    --config-path="$CONFIG_PATH" \
    --config-name='gsm8k_multiturn_grpo' \
    algorithm.adv_estimator=grpo \
    data.train_batch_size=64 \
    data.max_prompt_length=4096 \
    data.max_response_length=30000 \
    data.filter_overlong_prompts=True \
    data.truncation='error' \
    data.return_raw_chat=True \
    actor_rollout_ref.model.path="$MODEL_PATH" \
    actor_rollout_ref.actor.optim.lr=1e-6 \
    actor_rollout_ref.model.use_remove_padding=True \
    actor_rollout_ref.actor.ppo_mini_batch_size=64 \
    actor_rollout_ref.actor.ppo_micro_batch_size_per_gpu=8 \
    actor_rollout_ref.actor.use_kl_loss=True \
    actor_rollout_ref.actor.kl_loss_coef=0.001 \
    actor_rollout_ref.actor.kl_loss_type=low_var_kl \
    actor_rollout_ref.actor.entropy_coeff=0 \
    actor_rollout_ref.model.enable_gradient_checkpointing=True \
    actor_rollout_ref.actor.fsdp_config.param_offload=False \
    actor_rollout_ref.actor.fsdp_config.optimizer_offload=False \
    actor_rollout_ref.rollout.log_prob_micro_batch_size_per_gpu=16 \
    actor_rollout_ref.rollout.tensor_model_parallel_size=2 \
    actor_rollout_ref.rollout.name=sglang \
    actor_rollout_ref.rollout.gpu_memory_utilization=0.8 \
    actor_rollout_ref.rollout.n=16 \
    actor_rollout_ref.rollout.over_sample_rate=0.1 \
    actor_rollout_ref.rollout.mode=async \
    actor_rollout_ref.ref.log_prob_micro_batch_size_per_gpu=16 \
    actor_rollout_ref.ref.fsdp_config.param_offload=True \
    algorithm.use_kl_in_reward=False \
    trainer.critic_warmup=0 \
    trainer.logger='["console","wandb"]' \
    trainer.project_name='gsm8k_async_rl' \
    trainer.experiment_name="${EXP_NAME:-privacy_agent_grpo}" \
    trainer.n_gpus_per_node=4 \
    trainer.nnodes=1 \
    trainer.save_freq=10 \
    trainer.test_freq=20 \
    data.train_files="$TRAIN_FILE" \
    data.val_files="$VAL_FILE" \
    actor_rollout_ref.rollout.multi_turn.tool_config_path="$TOOL_CONFIG_PATH" \
    trainer.total_epochs=1 "$@"
