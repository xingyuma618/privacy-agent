#!/usr/bin/env python3
"""Run a deterministic fixed workflow with an execution-triggered SQL retry."""
from __future__ import annotations

# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))


import argparse
import concurrent.futures
import copy
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
LEGACY = Path(str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/ineference/agent_eval_multi_checkpoint_lmdeploy_token_usage_privacy_f1.py'))
TLS = threading.local()
WRITE_LOCK = threading.Lock()
ROUTE_LOCK = threading.Lock()
ROUTE_COUNTERS: dict[str, int] = {}


def load_legacy():
    sys.path.insert(0, str(LEGACY.parent))
    spec = importlib.util.spec_from_file_location("matched_fixed_legacy", LEGACY)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def safe_error(exc: Exception) -> dict[str, Any]:
    text = re.sub(r"(?i)(api[_ -]?key|authorization|bearer)[^,;\n]*", r"\1=<redacted>", str(exc))
    return {"error_type": type(exc).__name__, "status_code": getattr(exc, "status_code", None), "error_summary": text[:1000]}


def json_copy(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return json.loads(json.dumps(value, default=str))


class CompletionProxy:
    def __init__(self, inner, role: str, logical_endpoint: str, actual_endpoint: str):
        self.inner = inner
        self.role = role
        self.logical_endpoint = logical_endpoint
        self.actual_endpoint = actual_endpoint

    def create(self, *args, **kwargs):
        kwargs = dict(kwargs)
        if self.role == "sql_generator":
            kwargs["extra_body"] = {"chat_template_kwargs": {"enable_thinking": False}}
        started = time.time()
        record = {
            "role": self.role,
            "logical_endpoint": self.logical_endpoint,
            "endpoint": self.actual_endpoint,
            "request_model": str(kwargs.get("model", "")),
            "messages": json_copy(kwargs.get("messages", [])),
            "temperature": kwargs.get("temperature"),
            "max_tokens": kwargs.get("max_tokens"),
            "started_unix": started,
        }
        try:
            response = self.inner.create(*args, **kwargs)
            usage = getattr(response, "usage", None)
            record.update(
                ok=True,
                response_id=getattr(response, "id", None) or getattr(response, "_request_id", None),
                response_model=getattr(response, "model", None),
                finish_reason=(getattr(response.choices[0], "finish_reason", None) if getattr(response, "choices", None) else None),
                usage={k: getattr(usage, k, None) for k in ("prompt_tokens", "completion_tokens", "total_tokens")} if usage else {},
            )
            return response
        except Exception as exc:
            record.update(ok=False, response_id=None, response_model=None, usage={}, **safe_error(exc))
            raise
        finally:
            record["elapsed_seconds"] = time.time() - started
            if getattr(TLS, "trace", None) is not None:
                TLS.trace.append(record)


class ChatProxy:
    def __init__(self, inner, role: str, logical_endpoint: str, actual_endpoint: str):
        self._inner = inner
        self.completions = CompletionProxy(inner.completions, role, logical_endpoint, actual_endpoint)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class ClientProxy:
    def __init__(self, inner, role: str, logical_endpoint: str, actual_endpoint: str):
        self._inner = inner
        self.chat = ChatProxy(inner.chat, role, logical_endpoint, actual_endpoint)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def routed_factory(real, route_by_logical: dict[str, tuple[str, list[str]]]):
    normalized = {key.rstrip("/"): value for key, value in route_by_logical.items()}

    def factory(*args, **kwargs):
        logical = str(kwargs.get("base_url", "")).rstrip("/")
        if logical not in normalized:
            raise RuntimeError(f"unapproved model endpoint rejected: {logical}")
        role, replicas = normalized[logical]
        with ROUTE_LOCK:
            offset = ROUTE_COUNTERS.get(role, 0)
            ROUTE_COUNTERS[role] = offset + 1
        actual = replicas[offset % len(replicas)].rstrip("/")
        new_kwargs = dict(kwargs)
        new_kwargs["base_url"] = actual
        return ClientProxy(real(*args, **new_kwargs), role, logical, actual)

    return factory


def verify_services(models: dict[str, Any]) -> dict[str, list[str]]:
    observed = {}
    for role, spec in models.items():
        for endpoint in spec["endpoints"]:
            with urllib.request.urlopen(endpoint.rstrip("/") + "/models", timeout=10) as response:
                ids = [str(row["id"]) for row in json.load(response).get("data", [])]
            if spec["served_model_name"] not in ids:
                raise RuntimeError(f"{role} at {endpoint} exposes {ids}, expected {spec['served_model_name']}")
            observed[endpoint] = ids
    return observed


def find_db(root: str, db_id: str) -> Path:
    for candidate in (Path(root) / db_id / f"{db_id}.sqlite", Path(root) / db_id / f"{db_id}.db"):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"database file absent for {db_id}")


def execute_readonly(sql: str, db_path: Path, timeout: float) -> list[tuple]:
    if not str(sql or "").strip():
        raise ValueError("empty SQL")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=timeout)
    try:
        conn.execute("PRAGMA query_only=ON")
        timer = threading.Timer(timeout, conn.interrupt)
        timer.start()
        try:
            return conn.execute(sql.strip().rstrip(";")).fetchall()
        finally:
            timer.cancel()
    finally:
        conn.close()


def evaluate_ex(pred: str, gold: str, db_path: Path, timeout: float) -> tuple[int, dict[str, Any] | None, str | None]:
    try:
        gold_rows = execute_readonly(gold, db_path, timeout)
    except Exception as exc:
        return 0, safe_error(exc), "gold_or_database"
    try:
        pred_rows = execute_readonly(pred, db_path, timeout)
        return int(set(pred_rows) == set(gold_rows)), None, None
    except Exception as exc:
        return 0, safe_error(exc), "predicted_sql"


def request_costs(trace: list[dict[str, Any]]) -> dict[str, int]:
    roles = ("query_rewrite", "entity_extract", "sql_generator")
    totals = {role: 0 for role in roles}
    for request in trace:
        role = request.get("role", "unknown")
        totals.setdefault(role, 0)
        totals[role] += int((request.get("usage") or {}).get("total_tokens") or 0)
    totals["all"] = sum(totals.values())
    return totals


def remask_feedback(text: str, unmask_dict: dict[str, Any]) -> str:
    """Replace raw identifiers/values in local SQLite feedback with their masks."""
    replacements = []
    for category in ("tables", "columns", "values"):
        for mask, original in (unmask_dict.get(category) or {}).items():
            original_text = str(original)
            replacements.append((original_text, str(mask)))
            if category == "columns" and "." in original_text:
                replacements.append((original_text.split(".", 1)[1], str(mask)))
    output = str(text or "")
    for original, mask in sorted(set(replacements), key=lambda pair: len(pair[0]), reverse=True):
        if original:
            output = re.sub(re.escape(original), mask, output, flags=re.IGNORECASE)
    return output


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def flatten_bird_links(row: dict[str, Any]) -> dict[str, str]:
    links: dict[str, str] = {}
    for key in ("orig_gold_schema_links", "orig_gold_value_links"):
        raw = row.get(key) or {}
        if isinstance(raw, dict):
            for surface, target in raw.items():
                if str(surface).strip() and str(target).strip():
                    links[str(surface).strip()] = str(target).strip()
    return links


def load_dataset(module, spec: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    data = module.read_data(spec["source"])
    loader = spec["loader"]
    if loader == "masksql_subset":
        data, report = module.filter_and_attach_masksql_annotations(
            data, spec["masksql_annotations"], spec["match_mode"]
        )
        canonical = [
            str(row["idx"])
            for row in json.loads(Path(spec["masksql_annotations"]).read_text(encoding="utf-8"))
        ]
    elif loader == "bird_full":
        annotation_rows = [
            json.loads(line)
            for line in Path(spec["privacy_annotations"]).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        annotations = {
            str(row["idx"]).replace("bird_", ""): flatten_bird_links(row)
            for row in annotation_rows
        }
        for row in data:
            qid = str(row["question_id"])
            if qid not in annotations:
                raise RuntimeError(f"missing BIRD privacy annotation for question_id={qid}")
            row["masksql_idx"] = f"bird_full_{qid}"
            row["annotated_links"] = annotations[qid]
        data.sort(key=lambda row: int(row["question_id"]))
        canonical = [str(row["masksql_idx"]) for row in data]
        report = {"method": "question_id", "matched": len(data)}
    elif loader == "enriched_jsonl":
        missing = [i for i, row in enumerate(data) if not row.get("masksql_idx")]
        if missing:
            raise RuntimeError(f"enriched dataset lacks masksql_idx at row {missing[0]}")
        canonical = [str(row["masksql_idx"]) for row in data]
        report = {"method": "pre_enriched", "matched": len(data)}
    else:
        raise ValueError(f"unsupported dataset loader: {loader}")
    if len(data) != int(spec["expected_cases"]):
        raise RuntimeError(f"loaded {len(data)} cases, expected {spec['expected_cases']}")
    return data, canonical, report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("variant", choices=("one_pass_fixed", "iterative_fixed"))
    parser.add_argument("run", type=int, choices=(1, 2, 3))
    parser.add_argument(
        "--dataset",
        choices=("masksql300", "bird_dev_full", "spider_dev", "spider_test"),
        default="masksql300",
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    manifest_bytes = (HERE / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    # Every approved endpoint in this experiment is loopback-only.  Some login
    # shells export ALL_PROXY, which older httpx versions may consult even when
    # NO_PROXY contains 127.0.0.1; keep local model traffic off that proxy.
    for proxy_name in (
        "ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy",
        "HTTPS_PROXY", "https_proxy",
    ):
        os.environ.pop(proxy_name, None)
    os.environ["NO_PROXY"] = "localhost,127.0.0.1,::1"
    os.environ["no_proxy"] = "localhost,127.0.0.1,::1"
    output = Path(args.output)
    if output.exists() and any(output.iterdir()):
        raise SystemExit("output directory must be fresh and empty")
    output.mkdir(parents=True, exist_ok=True)
    (output / "case_logs").mkdir()

    observed = verify_services(manifest["models"])
    inf = manifest["inference"]
    models = manifest["models"]
    logical = {role: spec["endpoints"][0] for role, spec in models.items()}
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
    )

    module = load_legacy()
    route_spec = {
        logical[role]: (role, spec["endpoints"])
        for role, spec in models.items()
    }
    module.OpenAI = routed_factory(module.OpenAI, route_spec)
    dataset_spec = manifest["datasets"][args.dataset]
    data, canonical, match_report = load_dataset(module, dataset_spec)
    by_id = {str(row["masksql_idx"]): row for row in data}
    selected_ids = canonical[: args.limit or None]
    selected = [by_id[case_id] for case_id in selected_ids]

    agent = module.PrivacyProtectionAgent(
        base_url=logical["query_rewrite"],
        model_name=models["query_rewrite"]["served_model_name"],
        db_base_dir=dataset_spec["runtime_db_root"],
        log_dir=str(output / "case_logs"),
    )
    max_attempts = 1 if args.variant == "one_pass_fixed" else int(inf["max_sql_attempts"])
    metadata = {
        "status": "running",
        "variant": args.variant,
        "dataset": args.dataset,
        "run": args.run,
        "workers": args.workers,
        "limit": args.limit,
        "question_ids": selected_ids,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "legacy_evaluator_sha256": hashlib.sha256(LEGACY.read_bytes()).hexdigest(),
        "served_models_pre_run": observed,
        "match_report": match_report,
        "max_sql_attempts": max_attempts,
        "started_unix": time.time(),
    }
    write_json(output / "metadata.json", metadata)
    cases_path = output / "cases.jsonl"

    def one(pair):
        index, item = pair
        case_id = str(item["masksql_idx"])
        TLS.trace = []
        tool_calls = []
        db_path = find_db(dataset_spec["runtime_db_root"], item["db_id"])
        try:
            execute_readonly(item["gold_sql"], db_path, float(inf["final_eval_timeout_sec"]))
        except Exception as exc:
            return {
                "question_id": case_id, "case_id": case_id, "execution_correct": 0,
                "task_status": "infrastructure_error", "failure_kind": "gold_or_database",
                "trace_complete": True, "all_model_requests": [], "cloud_requests": [],
                "cost_tokens_by_role": request_costs([]), "cost_tokens": 0,
                "execution_error": safe_error(exc), "db_id": item["db_id"],
                "gold_sql": item["gold_sql"], "pred_sql": "",
                "annotated_links": item.get("annotated_links", {}), "masked_terms": [],
                "tool_calls": [],
            }
        original = f"{item['question']} {item.get('evidence', '')}".strip()
        try:
            tool_calls.append("query_rewrite")
            rewritten = agent._tool_query_rewrite(item.get("db_schema", ""), original)
            tool_calls.append("entity_extract")
            entity_text = agent._tool_entity_extract(item.get("db_schema", ""), rewritten)
            entities = module.json_repair.loads(entity_text) if entity_text else []
            if not isinstance(entities, list):
                entities = []
            tool_calls.append("apply_typed_masking")
            masked = agent._tool_apply_typed_masking(rewritten, entities, item.get("structured_db_schema", {}))
            masked_query = masked.get("masked_query", "")
            masked_schema = masked.get("masked_schema_str", "")
            unmask_dict = masked.get("unmask_dict", {})
            masked_terms = masked.get("masked_terms", [])
            masked_sql = ""
            pred_sql = ""
            execution_feedback = ""
            for attempt in range(max_attempts):
                retry_suffix = ""
                if attempt:
                    retry_suffix = (
                        "\n\nThe previous masked SQL failed the local execution check. "
                        "Correct it using the sanitized feedback below. Return one corrected SQL statement only."
                        f"\nPrevious masked SQL: {masked_sql}"
                        f"\nSanitized execution feedback: {remask_feedback(execution_feedback, unmask_dict)}"
                    )
                tool_calls.append("sql_generate")
                raw_sql = agent._tool_sql_generate(masked_query + retry_suffix, masked_schema)
                masked_sql = module.find_result(raw_sql)
                tool_calls.append("sql_unmask")
                pred_sql = agent._tool_sql_unmask(masked_sql, unmask_dict)
                tool_calls.append("execute_sql_check")
                try:
                    execute_readonly(pred_sql, db_path, float(inf["execution_timeout_sec"]))
                    execution_feedback = "Success"
                except Exception as exc:
                    execution_feedback = f"SQL Execution Error: {exc}"
                if execution_feedback == "Success":
                    break
            trace = list(TLS.trace)
            ex, ex_error, ex_failure = evaluate_ex(
                pred_sql, item["gold_sql"], db_path, float(inf["final_eval_timeout_sec"])
            )
            failed_calls = [call for call in trace if not call.get("ok")]
            task_status = "infrastructure_error" if failed_calls else "valid"
            failure_kind = "model_api" if failed_calls else ex_failure
            cloud = [call for call in trace if call.get("role") == "sql_generator"]
            return {
                "question_id": case_id, "case_id": case_id, "execution_correct": ex,
                "task_status": task_status, "failure_kind": failure_kind,
                "trace_complete": True, "all_model_requests": trace, "cloud_requests": cloud,
                "cost_tokens_by_role": request_costs(trace), "cost_tokens": request_costs(trace)["all"],
                "execution_error": ex_error, "db_id": item["db_id"],
                "gold_sql": item["gold_sql"], "pred_sql": pred_sql,
                "final_question": rewritten, "masked_question": masked_query,
                "masked_schema": masked_schema, "masked_sql": masked_sql,
                "annotated_links": item.get("annotated_links", {}), "masked_terms": masked_terms,
                "tool_calls": tool_calls, "num_tool_calls": len(tool_calls),
                "sql_attempts": tool_calls.count("sql_generate"),
                "execution_feedback": execution_feedback,
            }
        except Exception as exc:
            trace = list(TLS.trace)
            return {
                "question_id": case_id, "case_id": case_id, "execution_correct": 0,
                "task_status": "infrastructure_error", "failure_kind": "pipeline_exception",
                "trace_complete": True, "all_model_requests": trace,
                "cloud_requests": [call for call in trace if call.get("role") == "sql_generator"],
                "cost_tokens_by_role": request_costs(trace), "cost_tokens": request_costs(trace)["all"],
                "execution_error": safe_error(exc), "db_id": item["db_id"],
                "gold_sql": item["gold_sql"], "pred_sql": "",
                "annotated_links": item.get("annotated_links", {}), "masked_terms": [],
                "tool_calls": tool_calls, "num_tool_calls": len(tool_calls),
            }

    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(one, pair) for pair in enumerate(selected)]
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            with WRITE_LOCK:
                with cases_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                completed += 1
                metadata["completed_cases"] = completed
                write_json(output / "metadata.json", metadata)

    metadata.update(status="complete", completed_cases=completed, finished_unix=time.time())
    write_json(output / "metadata.json", metadata)


if __name__ == "__main__":
    main()
