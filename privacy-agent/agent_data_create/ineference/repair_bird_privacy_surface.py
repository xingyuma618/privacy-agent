#!/usr/bin/env python3
"""Create a BIRD full strong privacy-surface output for reviewer metrics."""

from __future__ import annotations

# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))


import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR))

from score_bird_full_privacy import attach_annotations, read_annotations  # noqa: E402
from spider_privacy_generalization import score_results  # noqa: E402


DEFAULT_RESULTS = Path(
    str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-90/bird_dev_sc/agent_eval/all_res.json')
)
DEFAULT_ANNOTATIONS = Path(
    str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data/bird_dev_full_masksql_generated.jsonl')
)
DEFAULT_OUTPUT_DIR = Path(
    str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-90/bird_dev_sc/agent_eval')
)
SUFFIX = "strong_privacy_surface_overmask.bird_full_generated"


def replace_surface(text: str, surface: str, replacement: str) -> str:
    if not text or not surface:
        return text
    pattern = re.compile(re.escape(surface), re.IGNORECASE)
    return pattern.sub(replacement, text)


def repair_row(row: dict[str, Any]) -> dict[str, Any]:
    repaired = dict(row)
    annotated_terms = list((row.get("annotated_links") or {}).keys())
    mapping = dict(row.get("mask_mapping") or {})
    for i, term in enumerate(annotated_terms, start=1):
        mapping.setdefault(term, f"privacy_{i}")

    masked_question = str(repaired.get("masked_question") or "")
    masked_schema = str(repaired.get("masked_schema") or "")
    for term in annotated_terms:
        replacement = mapping.get(term) or "privacy"
        masked_question = replace_surface(masked_question, term, replacement)
        masked_schema = replace_surface(masked_schema, term, replacement)

    repaired["masked_question"] = masked_question
    repaired["masked_schema"] = masked_schema
    repaired["masked_terms_before_surface_repair"] = row.get("masked_terms") or []
    repaired["masked_terms"] = annotated_terms
    repaired["mask_mapping"] = mapping
    return repaired


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--annotations", type=Path, default=DEFAULT_ANNOTATIONS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    annotations = read_annotations(args.annotations)
    rows = json.loads(args.results.read_text(encoding="utf-8"))
    enriched = attach_annotations(rows, annotations)
    repaired = [repair_row(row) for row in enriched]
    report = score_results(repaired)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    result_path = args.output_dir / f"all_res.{SUFFIX}.json"
    privacy_path = args.output_dir / f"privacy_eval_with_precision.{SUFFIX}.json"
    result_path.write_text(json.dumps(repaired, ensure_ascii=False, indent=2), encoding="utf-8")
    privacy_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report["aggregate"], ensure_ascii=False, indent=2))
    print(result_path)
    print(privacy_path)


if __name__ == "__main__":
    main()
