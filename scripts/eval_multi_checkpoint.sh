#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVAL_DIR="$ROOT/privacy-agent/agent_data_create/ineference"

cd "$EVAL_DIR"
bash run_multi_checkpoint_eval_lmdeploy_token_usage_privacy_f1.sh "$@"
