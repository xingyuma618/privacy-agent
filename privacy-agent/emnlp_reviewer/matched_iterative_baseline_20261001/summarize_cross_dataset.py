#!/usr/bin/env python3
"""Compare adaptive SFT with fixed retry on the same 300 cases per dataset."""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

HERE = Path(__file__).resolve().parent
from summarize import case_metrics


def read(path: Path) -> dict[str, dict]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return {str(row["case_id"]): row for row in rows}


def aggregate(rows: list[dict]) -> dict:
    valid = [row for row in rows if row.get("task_status") == "valid"]
    metrics = [case_metrics(row) for row in rows]
    return {
        "n": len(rows), "valid": len(valid),
        "EX": sum(x["EX"] for x in metrics) / len(metrics),
        "bounded_MP": sum(x["bounded_MP"] for x in metrics) / len(metrics),
        "bounded_MR": sum(x["bounded_MR"] for x in metrics) / len(metrics),
        "bounded_MF1": sum(x["bounded_MF1"] for x in metrics) / len(metrics),
        "tokens_per_case": sum(x["cost_tokens"] for x in metrics) / len(metrics),
        "tool_calls_per_case": sum(x["tool_calls"] for x in metrics) / len(metrics),
        "sql_attempts_per_case": sum(x["sql_attempts"] for x in metrics) / len(metrics),
    }


def paired_bootstrap_ex(adaptive: dict[str, dict], fixed: dict[str, dict], ids: list[str], seed: int) -> list[float]:
    deltas = [float(adaptive[x]["execution_correct"]) - float(fixed[x]["execution_correct"]) for x in ids]
    rng = random.Random(seed)
    draws = sorted(sum(deltas[rng.randrange(len(deltas))] for _ in deltas) / len(deltas) for _ in range(10000))
    return [draws[249], draws[9749]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=HERE / "cross_dataset_summary.json")
    args = parser.parse_args()
    manifest = json.loads((HERE / "manifest.json").read_text(encoding="utf-8"))
    report = {"status": "complete", "cases_per_dataset": 300, "runs": 1, "datasets": {}}
    for dataset in ("spider_dev", "spider_test"):
        adaptive_dir = HERE / "results_adaptive_generalization_4rep" / dataset / "sft" / "run-1"
        metadata = json.loads((adaptive_dir / "metadata.json").read_text(encoding="utf-8"))
        ids = [str(case_id) for case_id in metadata["question_ids"]]
        adaptive = read(adaptive_dir / "cases.jsonl")
        fixed = read(HERE / "results_generalization" / dataset / "iterative_fixed" / "run-1" / "cases.jsonl")
        if set(adaptive) != set(ids) or not set(ids).issubset(fixed):
            raise ValueError(f"case-ID mismatch for {dataset}")
        a = aggregate([adaptive[x] for x in ids])
        f = aggregate([fixed[x] for x in ids])
        common = [
            x for x in ids
            if adaptive[x].get("task_status") == "valid" and fixed[x].get("task_status") == "valid"
        ]
        common_a = aggregate([adaptive[x] for x in common])
        common_f = aggregate([fixed[x] for x in common])
        report["datasets"][dataset] = {
            "adaptive_sft": a, "iterative_fixed": f,
            "common_valid_comparison": {
                "n": len(common),
                "adaptive_EX": common_a["EX"],
                "iterative_fixed_EX": common_f["EX"],
                "adaptive_minus_fixed_EX": common_a["EX"] - common_f["EX"],
                "paired_question_bootstrap_95ci": paired_bootstrap_ex(
                    adaptive, fixed, common, 20261002 + len(report["datasets"])
                ),
            },
        }
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
