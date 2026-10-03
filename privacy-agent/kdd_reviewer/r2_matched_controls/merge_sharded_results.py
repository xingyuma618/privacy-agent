#!/usr/bin/env python3
"""Merge the two non-overlapping 150-case evaluation shards."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent / "results_300_sharded"
SHARDS = ("first_150", "last_150")
METHODS = ("r2_prompt_synthesis_n3101", "r2_mcts_synthesis_n3101")
REL = Path("bird_dev_sc/agent_eval")


def load(path):
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)


def weighted(rows, key):
    pairs = [(row.get(key), row.get("num_cases", 0)) for row in rows]
    pairs = [(value, count) for value, count in pairs if value is not None]
    total = sum(count for _, count in pairs)
    return sum(value * count for value, count in pairs) / total if total else None


summary = {}
for method in METHODS:
    inputs = [ROOT / shard / method / REL for shard in SHARDS]
    merged = ROOT / "merged" / method / REL

    all_res = sum((load(path / "all_res.json") for path in inputs), [])
    exec_rows = sum((load(path / "exec_result.json") for path in inputs), [])
    predictions = {}
    for path in inputs:
        predictions.update(load(path / "predict.json"))

    original_ids = [int(row["original_idx"]) for row in exec_rows]
    if len(all_res) != 300 or len(exec_rows) != 300 or len(predictions) != 300:
        raise ValueError(
            f"{method}: expected 300 rows, got all_res={len(all_res)}, "
            f"exec={len(exec_rows)}, predict={len(predictions)}"
        )
    if len(set(original_ids)) != 300:
        raise ValueError(f"{method}: duplicate original BIRD indices across shards")

    all_res.sort(key=lambda row: int(row["eval_original_idx"]))
    exec_rows.sort(key=lambda row: int(row["original_idx"]))
    predictions = dict(sorted(predictions.items(), key=lambda pair: int(pair[0])))
    dump(merged / "all_res.json", all_res)
    dump(merged / "exec_result.json", exec_rows)
    dump(merged / "predict.json", predictions)

    privacy = [load(path / "privacy_eval.json")["aggregate"] for path in inputs]
    tools = [load(path / "tool_use_audit.json") for path in inputs]
    tokens = [load(path / "token_usage.json") for path in inputs]
    correct = sum(int(row.get("res", 0)) for row in exec_rows)

    method_summary = {
        "num_cases": 300,
        "execution_correct": correct,
        "EX": correct / 300,
        "privacy": {
            key: weighted(privacy, key)
            for key in ("MR", "MR_present", "Privacy-P", "Privacy-R", "Privacy-F1", "LeakRate", "prompt_privacy_score", "RI")
        },
        "tool_use": {
            key: weighted(tools, key)
            for key in ("avg_tool_calls_per_case", "case_completed_by_format_rate", "case_timeout_rate", "function_name_accuracy", "argument_validity_rate")
        },
        "tokens": {
            "total_tokens_for_sql_generation": sum(row["total_tokens_for_sql_generation"] for row in tokens),
            "average_total_tokens_per_sql_generation": weighted(
                [{"num_cases": row["num_cases"], "value": row["average_total_tokens_per_sql_generation"]} for row in tokens],
                "value",
            ),
        },
        "shards": [str(path) for path in inputs],
    }
    summary[method] = method_summary
    dump(merged / "merged_summary.json", method_summary)

dump(ROOT / "merged_summary.json", summary)
print(json.dumps(summary, ensure_ascii=False, indent=2))
