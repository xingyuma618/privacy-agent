#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LLAMAFACTORY_ROOT="$ROOT/LlamaFactory"
TASK="${1:-agent}"
BASE_MODEL="${BASE_MODEL:-$ROOT/models/Qwen3-4B-Instruct}"

case "$TASK" in
  entity|entity_extract)
    CONFIG="$LLAMAFACTORY_ROOT/examples/train_full/qwen3_entity_extract.yaml"
    RUNTIME="$LLAMAFACTORY_ROOT/examples/train_full/runtime_qwen3_entity_extract.yaml"
    ;;
  query|query_rewrite)
    CONFIG="$LLAMAFACTORY_ROOT/examples/train_full/qwen3_query_write.yaml"
    RUNTIME="$LLAMAFACTORY_ROOT/examples/train_full/runtime_qwen3_query_write.yaml"
    ;;
  agent|agent_prompt)
    CONFIG="$LLAMAFACTORY_ROOT/examples/train_full/qwen3_agent.yaml"
    RUNTIME="$LLAMAFACTORY_ROOT/examples/train_full/runtime_qwen3_agent.yaml"
    ;;
  *)
    echo "Unknown task: $TASK"
    echo "Usage: BASE_MODEL=/path/to/Qwen3-4B-Instruct bash scripts/train_sft.sh [entity|query|agent]"
    exit 2
    ;;
esac

if [[ ! -d "$BASE_MODEL" ]]; then
  echo "Base model not found: $BASE_MODEL"
  echo "Put the base model under $ROOT/models/Qwen3-4B-Instruct or set BASE_MODEL=/path/to/model."
  exit 1
fi

python - "$CONFIG" "$RUNTIME" "$BASE_MODEL" "$TASK" <<'CONFIG'
import os, sys, yaml
source, target, model, task = sys.argv[1:]
with open(source) as f:
    config = yaml.safe_load(f)
config['model_name_or_path'] = model
if task == 'agent_prompt':
    config['dataset'] = 'nl2sql_agent_prompt'
    config['output_dir'] = 'saves/agent_prompt'
if os.getenv('CUTOFF_LEN'):
    config['cutoff_len'] = int(os.environ['CUTOFF_LEN'])
with open(target, 'w') as f:
    yaml.safe_dump(config, f, sort_keys=False, allow_unicode=True)
CONFIG

python "$ROOT/scripts/prepare_data.py"

cd "$LLAMAFACTORY_ROOT"
llamafactory-cli train "$RUNTIME"
