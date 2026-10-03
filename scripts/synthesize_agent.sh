#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PRIVACY_AGENT_RELEASE_ROOT="$ROOT"
case "${1:-mcts}" in
  prompt) ENTRY=agent.py ;;
  mcts) ENTRY=mcts_data_synthesis_eraly_stop.py ;;
  *) echo 'Usage: bash scripts/synthesize_agent.sh [prompt|mcts]' >&2; exit 2 ;;
esac
python "$ROOT/scripts/prepare_data.py"
cd "$ROOT/privacy-agent/agent_data_create"
python "$ENTRY"
