#!/usr/bin/env bash
set -euo pipefail

# =========================
# 批量 merge verl FSDP checkpoints 到 HuggingFace 格式
# 输出目录命名为 checkpoint-{step}，用于适配多 checkpoint 并行评估脚本
# =========================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERL_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 用法：EXP_NAME=reward_ablation bash examples/nl2sql/merge_verl_fsdp_all.sh
EXP_NAME="${EXP_NAME:-reward_ablation}"

# checkpoint 根目录
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-$VERL_ROOT/checkpoints/gsm8k_async_rl/${EXP_NAME}}"

# merge 哪个组件，一般是 actor
COMPONENT="${COMPONENT:-actor}"

# 输出目录：/mnt/.../Qwen3-4B-Instruct/{实验名}
OUTPUT_ROOT="${OUTPUT_ROOT:-$VERL_ROOT/output/models/qwen/Qwen3-4B-Instruct/${EXP_NAME}}"

# 是否覆盖已有 merge 结果
# 用法：OVERWRITE=1 bash merge.sh
OVERWRITE="${OVERWRITE:-0}"

mkdir -p "$OUTPUT_ROOT"

echo "============================================================"
echo "EXP_NAME       : $EXP_NAME"
echo "CHECKPOINT_ROOT: $CHECKPOINT_ROOT"
echo "COMPONENT      : $COMPONENT"
echo "OUTPUT_ROOT    : $OUTPUT_ROOT"
echo "OVERWRITE      : $OVERWRITE"
echo "============================================================"

python - <<'PY'
import importlib.util

spec = importlib.util.find_spec("verl.model_merger")
if spec is None:
    raise SystemExit(
        "ERROR: Cannot find verl.model_merger. "
        "Please run this script inside the verl environment/repo, "
        "or make sure verl is installed and PYTHONPATH is set."
    )

print("Found verl.model_merger")
PY

mapfile -t STEP_DIRS < <(find "$CHECKPOINT_ROOT" -maxdepth 1 -type d -name "global_step_*" | sort -V)

if [[ "${#STEP_DIRS[@]}" -eq 0 ]]; then
  echo "ERROR: No global_step_* directories found under: $CHECKPOINT_ROOT"
  exit 1
fi

echo "Found ${#STEP_DIRS[@]} global_step directories."
echo

for STEP_DIR in "${STEP_DIRS[@]}"; do
  STEP_NAME="$(basename "$STEP_DIR")"

  # global_step_20 -> 20
  STEP_NUM="${STEP_NAME#global_step_}"

  if [[ -z "$STEP_NUM" || "$STEP_NUM" == "$STEP_NAME" ]]; then
    echo "Skip: cannot parse step number from $STEP_NAME"
    continue
  fi

  LOCAL_DIR="${STEP_DIR}/${COMPONENT}"

  # 关键修改：
  # 原来是 global_step_20_actor_hf
  # 现在改成 checkpoint-20
  TARGET_DIR="${OUTPUT_ROOT}/checkpoint-${STEP_NUM}"

  echo "------------------------------------------------------------"
  echo "Step      : $STEP_NAME"
  echo "Step num  : $STEP_NUM"
  echo "Local dir : $LOCAL_DIR"
  echo "Target dir: $TARGET_DIR"

  if [[ ! -d "$LOCAL_DIR" ]]; then
    echo "Skip: local dir does not exist: $LOCAL_DIR"
    continue
  fi

  if [[ ! -f "${LOCAL_DIR}/fsdp_config.json" ]]; then
    echo "Skip: fsdp_config.json not found in $LOCAL_DIR"
    continue
  fi

  if ! ls "${LOCAL_DIR}"/model_world_size_*_rank_*.pt >/dev/null 2>&1; then
    echo "Skip: no model_world_size_*_rank_*.pt shards found in $LOCAL_DIR"
    continue
  fi

  if [[ -d "$TARGET_DIR" ]] && \
     { ls "$TARGET_DIR"/model*.safetensors >/dev/null 2>&1 || ls "$TARGET_DIR"/pytorch_model*.bin >/dev/null 2>&1; }; then
    if [[ "$OVERWRITE" != "1" ]]; then
      echo "Skip: merged model already exists in $TARGET_DIR"
      echo "Set OVERWRITE=1 to re-merge."
      continue
    fi
  fi

  if [[ "$OVERWRITE" == "1" ]]; then
    rm -rf "$TARGET_DIR"
  fi

  mkdir -p "$TARGET_DIR"

  echo "Merging..."
  python -m verl.model_merger merge \
    --backend fsdp \
    --local_dir "$LOCAL_DIR" \
    --target_dir "$TARGET_DIR"

  # 写一个元信息文件，方便之后追踪来源
  cat > "${TARGET_DIR}/merge_info.json" <<EOF
{
  "exp_name": "${EXP_NAME}",
  "source_step_dir": "${STEP_DIR}",
  "source_component": "${COMPONENT}",
  "source_step_name": "${STEP_NAME}",
  "output_checkpoint_name": "checkpoint-${STEP_NUM}"
}
EOF

  echo "Done: $TARGET_DIR"
  ls -lh "$TARGET_DIR" | head -50
done

echo
echo "============================================================"
echo "All done. Merged HuggingFace models are under:"
echo "$OUTPUT_ROOT"
echo
echo "You can use this directory as checkpoint_root in multi-checkpoint evaluation:"
echo "$OUTPUT_ROOT"
echo "============================================================"
