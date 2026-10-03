#!/usr/bin/env python3
"""Prepare full BIRD-dev generated annotations for the MaskSQL pipeline."""

from __future__ import annotations

# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))


import argparse
import json
import os
from pathlib import Path
from typing import Any


DEFAULT_ANNOTATIONS = Path(
    str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data/bird_dev_full_masksql_generated.jsonl')
)
DEFAULT_TABLES = Path(str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/dev/dev_tables.json'))
DEFAULT_DB_ROOT = Path(str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/dev/dev_databases'))
DEFAULT_OUTPUT = Path(str(_RELEASE_ROOT / 'privacy-agent/MaskSQL/data_bird_dev_generated'))


def read_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return json.loads(path.read_text(encoding="utf-8"))


def flatten_links(row: dict[str, Any]) -> dict[str, str]:
    links: dict[str, str] = {}
    for key in ["orig_gold_schema_links", "orig_gold_value_links"]:
        raw = row.get(key) or {}
        if not isinstance(raw, dict):
            continue
        for surface, target in raw.items():
            surface = str(surface).strip()
            target = str(target).strip()
            if surface and target:
                links[surface] = target
    return links


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_symlink(link_path: Path, target_path: Path) -> None:
    if link_path.exists() or link_path.is_symlink():
        if link_path.is_symlink() and Path(os.readlink(link_path)) == target_path:
            return
        raise FileExistsError(f"{link_path} already exists and is not the expected symlink")
    link_path.symlink_to(target_path, target_is_directory=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--annotations", type=Path, default=DEFAULT_ANNOTATIONS)
    parser.add_argument("--tables-json", type=Path, default=DEFAULT_TABLES)
    parser.add_argument("--db-root", type=Path, default=DEFAULT_DB_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--allow-partial", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = read_rows(args.annotations)
    if len(rows) != 1534 and not args.allow_partial:
        raise ValueError(f"expected 1534 BIRD-dev annotations, found {len(rows)}")

    input_rows = []
    for row in rows:
        input_rows.append(
            {
                "idx": str(row["idx"]).replace("bird_", ""),
                "db_id": row["orig_db_id"],
                "question": row["orig_question"],
                "query": row["orig_gold_sql"],
                "annotated_links": flatten_links(row),
            }
        )

    input_rows.sort(key=lambda item: int(item["idx"]))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.output_dir / "1_input.json", input_rows)
    write_json(args.output_dir / "tables.json", json.loads(args.tables_json.read_text(encoding="utf-8")))
    ensure_symlink(args.output_dir / "databases", args.db_root.resolve())

    report = {
        "annotations": str(args.annotations),
        "output_dir": str(args.output_dir),
        "num_rows": len(input_rows),
        "tables_json": str(args.tables_json),
        "db_root": str(args.db_root),
    }
    write_json(args.output_dir / "prepare_report.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
