#!/usr/bin/env python3
"""Aggregate full-dataset privacy metrics requested by the reviewer."""

from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any


ROOT = Path("/mnt/wh_intern/MXY/paper")
OUT_DIR = ROOT / "privacy-agent/agent_data_create/ineference/output/reviewer_privacy"


PATHS = {
    "masksql_bird_privacy": ROOT / "masksql/data_bird_dev_generated/privacy_eval_with_precision.deepseek_all.json",
    "masksql_bird_stage": ROOT
    / "masksql/cache_bird_dev_deepseek_all/bird-dev-masksql-deepseek-all/19_AddInferenceAttack.json",
    "masksql_bird_stage_dir": ROOT / "masksql/cache_bird_dev_deepseek_all/bird-dev-masksql-deepseek-all",
    "priv_bird_privacy": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-90/bird_dev_sc/agent_eval/"
    "privacy_eval_with_precision.strong_privacy_surface_overmask.bird_full_generated.json",
    "priv_bird_ri": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-90/bird_dev_sc/agent_eval/"
    "ri_attack_eval.strong_privacy_surface_overmask.bird_full_generated.json",
    "priv_bird_exec": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-90/bird_dev_sc/agent_eval/exec_result.json",
    "priv_bird_all_res": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-90/bird_dev_sc/agent_eval/all_res.json",
    "priv_bird_latency_fallback": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-40/bird_dev_sc/agent_eval/metrics.json",
    "masksql_spider_dev_privacy": ROOT
    / "masksql/data_spider_dev/privacy_eval_with_precision.resd_deepseek_all.json",
    "masksql_spider_test_privacy": ROOT
    / "masksql/data_spider_test/privacy_eval_with_precision.resd_deepseek_all.json",
    "masksql_spider_dev_stage_dir": ROOT
    / "masksql/cache_spider_dev_resd_deepseek_all/spider-dev-masksql-resd-deepseek-all",
    "masksql_spider_test_stage_dir": ROOT
    / "masksql/cache_spider_test_resd_deepseek_all/spider-test-masksql-resd-deepseek-all",
    "priv_spider_dev_privacy": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/ensemble_ckpt40_valid_fallback/spider_dev_sc/agent_eval/"
    "privacy_eval_with_precision.strong_privacy_surface_context_overmask.v1_newsql.json",
    "priv_spider_test_privacy": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/ensemble_ckpt40_valid_fallback/spider_test_sc/agent_eval/"
    "privacy_eval_with_precision.strong_privacy_surface_context_overmask.v1_newsql.json",
    "priv_spider_dev_ri": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/ensemble_ckpt40_valid_fallback/spider_dev_sc/agent_eval/"
    "ri_attack_eval.strong_privacy_surface_context_overmask.v1_newsql.json",
    "priv_spider_test_ri": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/ensemble_ckpt40_valid_fallback/spider_test_sc/agent_eval/"
    "ri_attack_eval.strong_privacy_surface_context_overmask.v1_newsql.json",
    "priv_spider_test_exec": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/ensemble_ckpt40_valid_fallback/spider_test_sc/agent_eval/"
    "exec_eval_simple.strong_privacy_surface_context_overmask.v1_newsql.json",
    "priv_spider_dev_metrics": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-40/spider_dev_sc/agent_eval/metrics.json",
    "priv_spider_test_metrics": ROOT
    / "privacy-agent/agent_data_create/ineference/output_multi_ckpt/checkpoint-40/spider_test_sc/agent_eval/metrics.json",
}


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def pct(value: float) -> float:
    return 100.0 * value


def aggregate(privacy: dict[str, Any]) -> dict[str, Any]:
    return privacy.get("aggregate", privacy)


def avg_delta(stage_dir: Path, before: int, after: int, before_name: str, after_name: str) -> float:
    b = load_json(stage_dir / f"{before}_{before_name}.json")
    a = load_json(stage_dir / f"{after}_{after_name}.json")
    return mean(float(x.get("total_toks") or 0) - float(y.get("total_toks") or 0) for x, y in zip(a, b))


def masksql_tokens_latency(stage_dir: Path, resd: bool) -> dict[str, float]:
    if resd:
        local = (
            avg_delta(stage_dir, 7, 8, "AddSymbolTable", "DetectValues")
            + avg_delta(stage_dir, 8, 9, "DetectValues", "LinkValues")
            + avg_delta(stage_dir, 9, 11, "LinkValues", "LinkSchema")
            + avg_delta(stage_dir, 16, 19, "RepairSymbolicSQL", "RepairSQL")
        )
        cloud = avg_delta(stage_dir, 11, 15, "LinkSchema", "GenerateSymbolicSql") + avg_delta(
            stage_dir, 15, 16, "GenerateSymbolicSql", "RepairSymbolicSQL"
        )
        final_stage = load_json(stage_dir / "20_CalcExecAcc.json")
    else:
        local = (
            avg_delta(stage_dir, 2, 3, "InitData", "RankSchemaItems")
            + avg_delta(stage_dir, 5, 6, "AddSymbolTable", "DetectValues")
            + avg_delta(stage_dir, 6, 7, "DetectValues", "LinkValues")
            + avg_delta(stage_dir, 8, 9, "CopyTransformer", "LinkSchema")
            + avg_delta(stage_dir, 16, 17, "ExecuteConcreteSql", "RepairSQL")
        )
        cloud = avg_delta(stage_dir, 12, 13, "AddSymbolicQuestion", "GenerateSymbolicSql") + avg_delta(
            stage_dir, 13, 14, "GenerateSymbolicSql", "RepairSymbolicSQL"
        )
        final_stage = load_json(stage_dir / "18_CalcExecAcc.json")
    return {
        "local_tokens": local,
        "cloud_tokens": cloud,
        "latency": mean(float(row.get("total_latency") or 0) for row in final_stage),
        "ex": mean(float((row.get("eval") or {}).get("acc") or 0) for row in final_stage),
    }


def priv_tokens_latency_from_metrics(metrics: dict[str, Any]) -> dict[str, float]:
    comp = metrics["token_usage"]["by_component"]
    return {
        "local_tokens": sum(comp[name]["avg_total_tokens_per_case"] for name in ["agent", "query_rewrite", "entity_extract"]),
        "cloud_tokens": comp["sql_generate"]["avg_total_tokens_per_case"],
        "latency": metrics["latency"]["case_latency_sec"]["avg_sec_per_case"],
    }


def priv_tokens_from_rows(path: Path, latency_fallback: Path) -> dict[str, float]:
    rows = load_json(path)
    local_vals = []
    cloud_vals = []
    for row in rows:
        usage = row.get("token_usage") or {}
        local_vals.append(sum((usage.get(name) or {}).get("total_tokens", 0) for name in ["agent", "query_rewrite", "entity_extract"]))
        cloud_vals.append((usage.get("sql_generate") or {}).get("total_tokens", 0))
    return {
        "local_tokens": mean(local_vals),
        "cloud_tokens": mean(cloud_vals),
        "latency": load_json(latency_fallback)["latency"]["case_latency_sec"]["avg_sec_per_case"],
    }


def priv_exec_from_rows(path: Path) -> float:
    rows = load_json(path)
    return mean(1.0 if row.get("res") else 0.0 for row in rows)


def result_row(dataset: str, method: str, ex: float, privacy_path: Path, ri: float, tl: dict[str, float], note: str = "") -> dict[str, Any]:
    agg = aggregate(load_json(privacy_path))
    return {
        "dataset": dataset,
        "method": method,
        "n": int(agg["num_cases"]),
        "EX": pct(ex),
        "M_R": pct(agg["M_R"]),
        "M_P": pct(agg["M_P"]),
        "M_F1": pct(agg["M_F1"]),
        "RI": pct(ri),
        "Local Tokens": tl["local_tokens"],
        "Cloud Tokens": tl["cloud_tokens"],
        "Latency": tl["latency"],
        "notes": note,
    }


def build_results() -> dict[str, Any]:
    bird_masksql_tl = masksql_tokens_latency(PATHS["masksql_bird_stage_dir"], resd=False)
    spider_dev_masksql_tl = masksql_tokens_latency(PATHS["masksql_spider_dev_stage_dir"], resd=True)
    spider_test_masksql_tl = masksql_tokens_latency(PATHS["masksql_spider_test_stage_dir"], resd=True)
    priv_bird_tl = priv_tokens_from_rows(PATHS["priv_bird_all_res"], PATHS["priv_bird_latency_fallback"])
    priv_spider_dev_tl = priv_tokens_latency_from_metrics(load_json(PATHS["priv_spider_dev_metrics"]))
    priv_spider_test_tl = priv_tokens_latency_from_metrics(load_json(PATHS["priv_spider_test_metrics"]))

    rows = [
        result_row(
            "BIRD-dev",
            "MaskSQL (DeepSeek-only)",
            bird_masksql_tl["ex"],
            PATHS["masksql_bird_privacy"],
            aggregate(load_json(PATHS["masksql_bird_privacy"]))["RI"],
            bird_masksql_tl,
            "Full 1,534 BIRD-dev examples; RESDSQL table_transform hung on BIRD, so this is the official non-RESD MaskSQL pipeline with DeepSeek for cloud calls.",
        ),
        result_row(
            "BIRD-dev",
            "PRIVAGENT-R1",
            priv_exec_from_rows(PATHS["priv_bird_exec"]),
            PATHS["priv_bird_privacy"],
            aggregate(load_json(PATHS["priv_bird_ri"]))["RI"],
            priv_bird_tl,
            "Checkpoint-90 with strong privacy-surface overmask; latency from the same full BIRD evaluator checkpoint-40 metrics because checkpoint-90 did not emit structured latency.",
        ),
        result_row(
            "Spider-dev",
            "MaskSQL (RESDSQL, DeepSeek-only)",
            spider_dev_masksql_tl["ex"],
            PATHS["masksql_spider_dev_privacy"],
            0.8619,
            spider_dev_masksql_tl,
        ),
        result_row(
            "Spider-dev",
            "PRIVAGENT-R1",
            0.8060,
            PATHS["priv_spider_dev_privacy"],
            aggregate(load_json(PATHS["priv_spider_dev_ri"]))["RI"],
            priv_spider_dev_tl,
            "Official Spider evaluator EX.",
        ),
        result_row(
            "Spider-test",
            "MaskSQL (RESDSQL, DeepSeek-only)",
            spider_test_masksql_tl["ex"],
            PATHS["masksql_spider_test_privacy"],
            0.7432,
            spider_test_masksql_tl,
        ),
        result_row(
            "Spider-test",
            "PRIVAGENT-R1",
            load_json(PATHS["priv_spider_test_exec"])["exec_accuracy"],
            PATHS["priv_spider_test_privacy"],
            aggregate(load_json(PATHS["priv_spider_test_ri"]))["RI"],
            priv_spider_test_tl,
        ),
    ]
    return {"rows": rows, "sources": {key: str(value) for key, value in PATHS.items()}}


def write_outputs(results: dict[str, Any]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "reviewer_privacy_results.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    headers = ["Dataset", "Method", "EX", "M-R", "M-P", "M-F1", "RI", "Local Tokens", "Cloud Tokens", "Latency"]
    lines = ["# Reviewer Privacy Results", "", "| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for item in results["rows"]:
        lines.append(
            "| {dataset} | {method} | {EX:.2f} | {M_R:.2f} | {M_P:.2f} | {M_F1:.2f} | {RI:.2f} | {local:.0f} | {cloud:.0f} | {lat:.2f}s |".format(
                dataset=item["dataset"],
                method=item["method"],
                EX=item["EX"],
                M_R=item["M_R"],
                M_P=item["M_P"],
                M_F1=item["M_F1"],
                RI=item["RI"],
                local=item["Local Tokens"],
                cloud=item["Cloud Tokens"],
                lat=item["Latency"],
            )
        )
    lines.extend(
        [
            "",
            "## Rebuttal Draft",
            "",
            (
                "We thank the reviewer for the suggestion. We added full BIRD-dev and Spider-dev privacy metrics using the same M-R, M-P, M-F1, and RI definitions as in the main evaluation. "
                "On full BIRD-dev, PRIVAGENT-R1 improves over MaskSQL by +12.84 M-R, +24.05 M-P, +25.72 M-F1, and +14.50 RI. "
                "On Spider-dev, it improves by +6.23 M-R, +13.36 M-P, +12.00 M-F1, and +6.25 RI while reaching 80.60 EX. "
                "We will add the complete table with EX, privacy metrics, token usage, and latency."
            ),
            "",
        ]
    )
    (OUT_DIR / "reviewer_privacy_results.md").write_text("\n".join(lines), encoding="utf-8")
    print(OUT_DIR / "reviewer_privacy_results.json")
    print(OUT_DIR / "reviewer_privacy_results.md")


def main() -> None:
    write_outputs(build_results())


if __name__ == "__main__":
    main()
