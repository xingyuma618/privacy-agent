#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERL_ROOT="$ROOT/privacy-agent/verl"

cd "$VERL_ROOT"
bash examples/sglang_multiturn/nl2sql.sh "$@"
