#!/usr/bin/env python3
"""Prepare Spider dev/test inputs for the official MaskSQL pipeline."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def flatten_annotated_links(row: dict[str, Any]) -> dict[str, Any]:
    annotated = row.get("annotated_links") or {}
    flattened: dict[str, Any] = {}
    for surface, meta in annotated.items():
        if isinstance(meta, dict):
            flattened[surface] = meta.get("target", meta.get("value", ""))
        else:
            flattened[surface] = meta
    return flattened


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def ensure_symlink(link_path: Path, target_path: Path) -> None:
    if link_path.exists() or link_path.is_symlink():
        if link_path.is_symlink() and Path(os.readlink(link_path)) == target_path:
            return
        raise FileExistsError(f"{link_path} already exists and is not the expected symlink")
    link_path.symlink_to(target_path, target_is_directory=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-jsonl", required=True, type=Path)
    parser.add_argument("--tables-json", required=True, type=Path)
    parser.add_argument("--db-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    rows = read_jsonl(args.input_jsonl)
    input_rows = []
    for i, row in enumerate(rows):
        input_rows.append(
            {
                "idx": str(row.get("question_id", i)),
                "db_id": row["db_id"],
                "question": row["question"],
                "query": row.get("gold_sql", ""),
                "annotated_links": flatten_annotated_links(row),
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.output_dir / "1_input.json", input_rows)
    write_json(args.output_dir / "tables.json", json.loads(args.tables_json.read_text()))
    ensure_symlink(args.output_dir / "databases", args.db_root.resolve())

    report = {
        "input_jsonl": str(args.input_jsonl),
        "tables_json": str(args.tables_json),
        "db_root": str(args.db_root),
        "output_dir": str(args.output_dir),
        "num_rows": len(input_rows),
    }
    write_json(args.output_dir / "prepare_report.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
