#!/usr/bin/env python3
"""Score full BIRD-dev privacy metrics from generated MaskSQL annotations."""

from __future__ import annotations

# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))


import argparse
import json
import sys
from pathlib import Path
from typing import Any


THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR))

from spider_privacy_generalization import score_results  # noqa: E402


DEFAULT_ANNOTATIONS = Path(
    str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data/bird_dev_full_masksql_generated.jsonl')
)
DEFAULT_RESULTS = Path(
    str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-40/bird_dev_sc/agent_eval/all_res.json')
)
DEFAULT_OUTPUT_DIR = Path(
    str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-40/bird_dev_sc/agent_eval')
)


def read_annotations(path: Path) -> dict[str, dict[str, str]]:
    rows = []
    if path.suffix == ".jsonl":
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        rows = json.loads(path.read_text(encoding="utf-8"))

    annotations: dict[str, dict[str, str]] = {}
    for row in rows:
        qid = str(row["idx"]).replace("bird_", "")
        links: dict[str, str] = {}
        for key in ("orig_gold_schema_links", "orig_gold_value_links"):
            raw = row.get(key) or {}
            if not isinstance(raw, dict):
                continue
            for surface, target in raw.items():
                surface = str(surface).strip()
                target = str(target).strip()
                if surface and target:
                    links[surface] = target
        annotations[qid] = links
    return annotations


def attach_annotations(results: list[dict[str, Any]], annotations: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    enriched = []
    missing = []
    for index, row in enumerate(results):
        qid = str(row.get("question_id") or row.get("eval_original_idx") or index)
        annotated_links = annotations.get(qid)
        if annotated_links is None:
            missing.append(qid)
            annotated_links = {}
        out = dict(row)
        out["annotated_links"] = annotated_links
        out["masksql_idx"] = qid
        out["case_id"] = str(out.get("case_id") or qid)
        enriched.append(out)
    if missing:
        raise ValueError(f"missing annotations for {len(missing)} results; first missing id={missing[0]}")
    return enriched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--annotations", type=Path, default=DEFAULT_ANNOTATIONS)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    annotations = read_annotations(args.annotations)
    results = json.loads(args.results.read_text(encoding="utf-8"))
    enriched = attach_annotations(results, annotations)
    report = score_results(enriched)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    enriched_path = args.output_dir / "all_res.with_bird_full_annotations.json"
    privacy_path = args.output_dir / "privacy_eval_with_precision.bird_full_generated.json"
    enriched_path.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")
    privacy_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report["aggregate"], ensure_ascii=False, indent=2))
    print(enriched_path)
    print(privacy_path)


if __name__ == "__main__":
    main()
