#!/usr/bin/env python3
"""Run the SFT adaptive controller on fixed subsets of the extra datasets."""
from __future__ import annotations

# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))


import argparse
import concurrent.futures
import hashlib
import json
import os
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import runner as fixed

TLS = fixed.TLS
WRITE_LOCK = fixed.WRITE_LOCK


def install_readonly_check(module) -> None:
    def check(self, unmasked_sql: str, db_path: str) -> str:
        if not unmasked_sql or not db_path:
            return "Error: Missing SQL or Database Path."
        try:
            fixed.execute_readonly(unmasked_sql, Path(db_path), 15)
            return "Success"
        except sqlite3.OperationalError as exc:
            if "interrupted" in str(exc).lower():
                return "SQL Execution Error: Execution timed out (exceeded 15 seconds)."
            return f"SQL Execution Error: {exc}"
        except Exception as exc:
            return f"SQL Execution Error: {exc}"
    module.PrivacyProtectionAgent._tool_execute_sql_check = check


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=("spider_dev", "spider_test"), required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()

    for name in ("ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
        os.environ.pop(name, None)
    os.environ.update(NO_PROXY="localhost,127.0.0.1,::1", no_proxy="localhost,127.0.0.1,::1")

    manifest_bytes = (HERE / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    dataset = manifest["datasets"][args.dataset]
    models = dict(manifest["models"])
    models["agent"] = {
        "path": str(_RELEASE_ROOT / 'LlamaFactory/saves/agent_v2_deepseek_pro/checkpoint-40'),
        "served_model_name": "PrivAgent-R1-SFT-checkpoint-40",
        "endpoints": [
            "http://127.0.0.1:18443/v1", "http://127.0.0.1:18453/v1",
            "http://127.0.0.1:18441/v1", "http://127.0.0.1:18451/v1",
        ],
    }
    observed = fixed.verify_services(models)
    logical = {role: spec["endpoints"][0] for role, spec in models.items()}
    inf = manifest["inference"]
    os.environ.update(
        QUERY_REWRITE_BASE_URL=logical["query_rewrite"],
        QUERY_REWRITE_MODEL_NAME=models["query_rewrite"]["served_model_name"],
        ENTITY_EXTRACT_BASE_URL=logical["entity_extract"],
        ENTITY_EXTRACT_MODEL_NAME=models["entity_extract"]["served_model_name"],
        SQL_GENERATE_BASE_URL=logical["sql_generator"],
        SQL_GENERATE_MODEL_NAME=models["sql_generator"]["served_model_name"],
        SQL_GENERATE_API_KEY="EMPTY",
        QUERY_REWRITE_MAX_TOKENS=str(inf["query_rewrite_max_tokens"]),
        ENTITY_EXTRACT_MAX_TOKENS=str(inf["entity_extract_max_tokens"]),
        SQL_GENERATE_MAX_TOKENS=str(inf["sql_generate_max_tokens"]),
        AGENT_MAX_TOKENS="2048",
    )
    module = fixed.load_legacy()
    route_spec = {
        logical[role]: (role, spec["endpoints"])
        for role, spec in models.items() if role != "agent"
    }
    # The agent client is long-lived, so route its two replicas explicitly and
    # choose an agent per case.  Tool clients may still round-robin at creation.
    for endpoint in models["agent"]["endpoints"]:
        route_spec[endpoint] = ("agent", [endpoint])
    module.OpenAI = fixed.routed_factory(module.OpenAI, route_spec)
    install_readonly_check(module)
    data, canonical, match_report = fixed.load_dataset(module, dataset)
    selected_ids = canonical[: args.limit]
    by_id = {str(row["masksql_idx"]): row for row in data}
    selected = [by_id[case_id] for case_id in selected_ids]

    output = Path(args.output)
    if output.exists() and any(output.iterdir()):
        raise SystemExit("output directory must be fresh and empty")
    output.mkdir(parents=True, exist_ok=True)
    (output / "case_logs").mkdir()
    metadata: dict[str, Any] = {
        "status": "running", "dataset": args.dataset, "limit": args.limit,
        "workers": args.workers, "question_ids": selected_ids,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "served_models_pre_run": observed, "match_report": match_report,
        "started_unix": time.time(),
    }
    fixed.write_json(output / "metadata.json", metadata)
    cases_path = output / "cases.jsonl"
    agents = [
        module.PrivacyProtectionAgent(
            endpoint, models["agent"]["served_model_name"],
            db_base_dir=dataset["runtime_db_root"], log_dir=str(output / "case_logs"),
        )
        for endpoint in models["agent"]["endpoints"]
    ]

    def one(pair):
        index, item = pair
        case_id = str(item["masksql_idx"])
        TLS.trace = []
        try:
            db_path = fixed.find_db(dataset["runtime_db_root"], item["db_id"])
            fixed.execute_readonly(item["gold_sql"], db_path, 30)
        except Exception as exc:
            return {
                "question_id": case_id, "case_id": case_id, "execution_correct": 0,
                "task_status": "infrastructure_error", "failure_kind": "gold_or_database",
                "trace_complete": True, "all_model_requests": [], "cloud_requests": [],
                "cost_tokens_by_role": fixed.request_costs([]), "cost_tokens": 0,
                "execution_error": fixed.safe_error(exc), "db_id": item["db_id"],
                "gold_sql": item["gold_sql"], "pred_sql": "",
                "annotated_links": item.get("annotated_links", {}), "masked_terms": [],
            }
        try:
            result = module.process_single_item(index, item, agents[index % len(agents)], len(selected))
            trace = list(TLS.trace)
            ex, ex_error, ex_failure = fixed.evaluate_ex(result["pred_sql"], result["gold_sql"], db_path, 30)
            failed = [call for call in trace if not call.get("ok")]
            audit = result.get("tool_use_audit") or {}
            status = "infrastructure_error" if failed else "valid"
            return {
                "question_id": case_id, "case_id": case_id, "execution_correct": ex,
                "task_status": status, "failure_kind": "model_api" if failed else ex_failure,
                "trace_complete": True, "all_model_requests": trace,
                "cloud_requests": [call for call in trace if call.get("role") == "sql_generator"],
                "cost_tokens_by_role": fixed.request_costs(trace),
                "cost_tokens": fixed.request_costs(trace)["all"], "execution_error": ex_error,
                "db_id": result["db_id"], "gold_sql": result["gold_sql"],
                "pred_sql": result["pred_sql"], "annotated_links": result["annotated_links"],
                "masked_terms": result["masked_terms"], "tool_use_audit": audit,
                "num_tool_calls": audit.get("num_tool_calls", 0),
                "sql_attempts": sum(call.get("role") == "sql_generator" for call in trace),
            }
        except Exception as exc:
            trace = list(TLS.trace)
            return {
                "question_id": case_id, "case_id": case_id, "execution_correct": 0,
                "task_status": "infrastructure_error", "failure_kind": "pipeline_exception",
                "trace_complete": True, "all_model_requests": trace,
                "cloud_requests": [call for call in trace if call.get("role") == "sql_generator"],
                "cost_tokens_by_role": fixed.request_costs(trace),
                "cost_tokens": fixed.request_costs(trace)["all"],
                "execution_error": fixed.safe_error(exc), "db_id": item["db_id"],
                "gold_sql": item["gold_sql"], "pred_sql": "",
                "annotated_links": item.get("annotated_links", {}), "masked_terms": [],
            }

    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for future in concurrent.futures.as_completed([pool.submit(one, pair) for pair in enumerate(selected)]):
            row = future.result()
            with WRITE_LOCK:
                with cases_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                    handle.flush(); os.fsync(handle.fileno())
                completed += 1
                metadata["completed_cases"] = completed
                fixed.write_json(output / "metadata.json", metadata)
    metadata.update(status="complete", completed_cases=completed, finished_unix=time.time())
    fixed.write_json(output / "metadata.json", metadata)


if __name__ == "__main__":
    main()
