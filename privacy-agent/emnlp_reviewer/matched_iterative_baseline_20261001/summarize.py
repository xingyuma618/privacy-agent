#!/usr/bin/env python3
"""Summarize fixed workflows and compare them with saved adaptive-agent runs."""
from __future__ import annotations

import argparse
import json
import math
import random
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
ADAPTIVE_ROOT = HERE.parent / "sft_vs_rl_20261001" / "results_local_qwen"
ROLES = ("agent", "query_rewrite", "entity_extract", "sql_generator")
METRICS = (
    "EX", "legacy_MR", "bounded_MP", "bounded_MR", "bounded_MF1",
    "cost_tokens", "tool_calls", "sql_attempts",
) + tuple(f"calls_{role}" for role in ROLES)


def norm(value):
    return " ".join(str(value).strip(" '\"`").lower().split())


def edge(gold, predicted):
    gold, predicted = norm(gold), norm(predicted)
    return bool(gold and predicted and (gold == predicted or gold in predicted or predicted in gold))


def unique_terms(values):
    output, seen = [], set()
    for value in values:
        key = norm(value)
        if key and key not in seen:
            seen.add(key)
            output.append(value)
    return output


def maximum_matching(gold, predicted):
    adjacent = [[j for j, term in enumerate(predicted) if edge(item, term)] for item in gold]
    match = {}

    def visit(i, seen):
        for j in adjacent[i]:
            if j in seen:
                continue
            seen.add(j)
            if j not in match or visit(match[j], seen):
                match[j] = i
                return True
        return False

    return sum(visit(i, set()) for i in range(len(gold)))


def case_metrics(row):
    gold_raw = list((row.get("annotated_links") or {}).keys())
    predicted_raw = row.get("masked_terms") or []
    covered = sum(any(edge(gold, predicted) for predicted in predicted_raw) for gold in gold_raw)
    gold, predicted = unique_terms(gold_raw), unique_terms(predicted_raw)
    matched = maximum_matching(gold, predicted)
    mp = matched / len(predicted) if predicted else (1.0 if not gold else 0.0)
    mr = matched / len(gold) if gold else 1.0
    requests = row.get("all_model_requests") or []
    metrics = {
        "EX": float(row.get("execution_correct", 0)),
        "legacy_MR": covered / len(gold_raw) if gold_raw else 1.0,
        "bounded_MP": mp,
        "bounded_MR": mr,
        "bounded_MF1": 2 * mp * mr / (mp + mr) if mp + mr else 0.0,
        "cost_tokens": float(row.get("cost_tokens", 0)),
        "tool_calls": float(row.get("num_tool_calls", (row.get("tool_use_audit") or {}).get("num_tool_calls", 0))),
        "sql_attempts": float(row.get("sql_attempts", sum(r.get("role") == "sql_generator" for r in requests))),
    }
    for role in ROLES:
        metrics[f"calls_{role}"] = float(sum(r.get("role") == role for r in requests))
    return metrics


def load_run(path, expected_ids):
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    by_id = {}
    for row in rows:
        case_id = row.get("case_id")
        if case_id in by_id:
            raise ValueError(f"duplicate case {case_id} in {path}")
        if row.get("task_status") != "valid":
            raise ValueError(f"infrastructure failure for {case_id} in {path}: {row.get('failure_kind')}")
        if row.get("trace_complete") is not True:
            raise ValueError(f"incomplete trace for {case_id} in {path}")
        by_id[case_id] = case_metrics(row)
    if set(by_id) != set(expected_ids):
        raise ValueError(f"canonical ID mismatch in {path}")
    return by_id


def average(values):
    return sum(values) / len(values)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir")
    parser.add_argument("--output", default=str(HERE / "summary.json"))
    parser.add_argument("--bootstrap", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=20261001)
    args = parser.parse_args()

    manifest = json.loads((HERE / "manifest.json").read_text())
    annotations = json.loads(Path(manifest["datasets"]["masksql300"]["masksql_annotations"]).read_text())
    ids = [str(row["idx"]) for row in annotations]
    roots = {
        "one_pass_fixed": Path(args.results_dir) / "one_pass_fixed",
        "iterative_fixed": Path(args.results_dir) / "iterative_fixed",
        "adaptive_sft": ADAPTIVE_ROOT / "sft",
        "adaptive_rl": ADAPTIVE_ROOT / "rl",
    }
    data = {
        name: [load_run(root / f"run-{run}" / "cases.jsonl", ids) for run in range(1, 4)]
        for name, root in roots.items()
    }
    per_run = {
        name: [
            {metric: average([run_data[case_id][metric] for case_id in ids]) for metric in METRICS}
            for run_data in runs
        ]
        for name, runs in data.items()
    }
    overall = {
        name: {
            metric: {
                "mean": average([run[metric] for run in runs]),
                "sample_sd": statistics.stdev([run[metric] for run in runs]),
            }
            for metric in METRICS
        }
        for name, runs in per_run.items()
    }

    rng = random.Random(args.seed)
    comparisons = {}
    for reference in ("one_pass_fixed", "adaptive_sft", "adaptive_rl"):
        label = f"iterative_fixed_minus_{reference}"
        comparisons[label] = {}
        for metric in METRICS:
            deltas = [
                average([data["iterative_fixed"][run][case_id][metric] for run in range(3)])
                - average([data[reference][run][case_id][metric] for run in range(3)])
                for case_id in ids
            ]
            draws = sorted(
                average([deltas[rng.randrange(len(deltas))] for _ in deltas])
                for _ in range(args.bootstrap)
            )
            lo = draws[math.floor(0.025 * (len(draws) - 1))]
            hi = draws[math.ceil(0.975 * (len(draws) - 1))]
            comparisons[label][metric] = {
                "delta": average(deltas),
                "paired_question_cluster_bootstrap_95ci": [lo, hi],
            }

    report = {
        "status": "complete",
        "n_cases": len(ids),
        "runs": 3,
        "definitions": {
            "comparison_unit": "Each question is averaged over three inference repetitions before bootstrap resampling.",
            "iterative_rule": "A second SQL generation/unmask/execution pass runs only after the first local execution check fails.",
            "budget_scope": "The fixed and adaptive methods use the same rewrite, entity and SQL-generator checkpoints. Actual calls and tokens are reported; controller-model tokens are not artificially added to the fixed workflow.",
            "privacy_metrics": "Surface-term matching only; these metrics do not measure semantic leakage.",
        },
        "per_run": per_run,
        "overall_across_run_means": overall,
        "paired_comparisons": comparisons,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
