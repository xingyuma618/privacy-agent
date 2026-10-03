#!/usr/bin/env python3
"""Summarize the single-run full-dataset checks without hiding failed cases."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
from summarize import case_metrics


def mean(rows: list[dict[str, Any]], field: str) -> float:
    return sum(float(row.get(field, 0) or 0) for row in rows) / len(rows)


def load_rows(path: Path, expected: int) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(rows) != expected:
        raise ValueError(f"{path}: expected {expected} cases, found {len(rows)}")
    return rows


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [row for row in rows if row.get("task_status") == "valid"]
    privacy = [case_metrics(row) for row in valid]
    return {
        "num_cases": len(rows),
        "num_valid": len(valid),
        "infrastructure_failures": len(rows) - len(valid),
        "failure_kinds": dict(Counter(row.get("failure_kind") for row in rows if row.get("task_status") != "valid")),
        "EX_all_cases": mean(rows, "execution_correct"),
        "EX_valid_cases": mean(valid, "execution_correct") if valid else None,
        "bounded_MP_valid_cases": sum(x["bounded_MP"] for x in privacy) / len(privacy) if privacy else None,
        "bounded_MR_valid_cases": sum(x["bounded_MR"] for x in privacy) / len(privacy) if privacy else None,
        "bounded_MF1_valid_cases": sum(x["bounded_MF1"] for x in privacy) / len(privacy) if privacy else None,
        "tokens_per_case": mean(rows, "cost_tokens"),
        "tool_calls_per_case": mean(rows, "num_tool_calls"),
        "sql_attempts_per_case": mean(rows, "sql_attempts"),
        "retry_rate": sum(float(row.get("sql_attempts", 0) > 1) for row in rows) / len(rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--output", type=Path, default=HERE / "generalization_summary.json")
    args = parser.parse_args()
    manifest = json.loads((HERE / "manifest.json").read_text(encoding="utf-8"))
    output: dict[str, Any] = {
        "status": "complete",
        "runs_per_dataset": 1,
        "note": "EX_all_cases keeps infrastructure failures in the denominator; privacy metrics are surface-term metrics, not semantic leakage metrics.",
        "datasets": {},
    }
    for dataset in ("bird_dev_full", "spider_dev", "spider_test"):
        expected = int(manifest["datasets"][dataset]["expected_cases"])
        variants = {}
        raw = {}
        for variant in ("one_pass_fixed", "iterative_fixed"):
            raw[variant] = load_rows(
                args.results_dir / dataset / variant / "run-1" / "cases.jsonl", expected
            )
            variants[variant] = summarize(raw[variant])
        by_variant = {
            name: {str(row["case_id"]): row for row in rows}
            for name, rows in raw.items()
        }
        common_ids = sorted(
            case_id for case_id in set(by_variant["one_pass_fixed"]) & set(by_variant["iterative_fixed"])
            if by_variant["one_pass_fixed"][case_id].get("task_status") == "valid"
            and by_variant["iterative_fixed"][case_id].get("task_status") == "valid"
        )
        common_one = [by_variant["one_pass_fixed"][case_id] for case_id in common_ids]
        common_iter = [by_variant["iterative_fixed"][case_id] for case_id in common_ids]
        common_one_ex = mean(common_one, "execution_correct")
        common_iter_ex = mean(common_iter, "execution_correct")
        output["datasets"][dataset] = {
            "one_pass_fixed": variants["one_pass_fixed"],
            "iterative_fixed": variants["iterative_fixed"],
            "common_valid_comparison": {
                "n": len(common_ids),
                "one_pass_EX": common_one_ex,
                "iterative_EX": common_iter_ex,
                "iterative_minus_one_pass_EX": common_iter_ex - common_one_ex,
            },
        }
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
