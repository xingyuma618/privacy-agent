#!/usr/bin/env python3
"""Run one immutable, incrementally persisted SFT-vs-RL repetition."""
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
TEMPLATE_DIR = LEGACY.parent / "template"
TLS = threading.local()
WRITE_LOCK = threading.Lock()


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()


def load_legacy():
    sys.path.insert(0, str(LEGACY.parent))
    spec = importlib.util.spec_from_file_location("paired_legacy_eval", LEGACY)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def json_copy(value: Any) -> Any:
    """Take the request snapshot without changing the object sent to the SDK."""
    try:
        return copy.deepcopy(value)
    except Exception:
        return json.loads(json.dumps(value, default=str))


def safe_error(exc: Exception) -> dict[str, Any]:
    status = getattr(exc, "status_code", None)
    message = re.sub(r"(?i)(api[_ -]?key|authorization|bearer)[^,;\n]*", r"\1=<redacted>", str(exc))
    return {"error_type": type(exc).__name__, "status_code": status, "error_summary": message[:1000]}


class CompletionProxy:
    def __init__(self, inner, endpoint: str, role: str):
        self.inner, self.endpoint, self.role = inner, endpoint, role

    def create(self, *args, **kwargs):
        if self.role == "sql_generator":
            # Match the historical direct-SQL generator behavior: do not spend
            # the fixed 1024-token answer budget on Qwen's optional thinking.
            kwargs = dict(kwargs)
            kwargs["extra_body"] = {"chat_template_kwargs": {"enable_thinking": False}}
        started = time.time()
        record = {
            "role": self.role,
            "endpoint": self.endpoint,
            "request_model": kwargs.get("model"),
            "messages": json_copy(kwargs.get("messages", [])),
            "temperature": kwargs.get("temperature"),
            "max_tokens": kwargs.get("max_tokens"),
            "extra_body": json_copy(kwargs.get("extra_body")),
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
            record.update(ok=False, response_model=None, response_id=None, usage={}, **safe_error(exc))
            raise
        finally:
            record["elapsed_seconds"] = time.time() - started
            trace = getattr(TLS, "trace", None)
            if trace is not None:
                trace.append(record)


class ChatProxy:
    def __init__(self, inner, endpoint: str, role: str):
        self._inner = inner
        self.completions = CompletionProxy(inner.completions, endpoint, role)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class ClientProxy:
    def __init__(self, inner, endpoint: str, role: str):
        self._inner = inner
        self.chat = ChatProxy(inner.chat, endpoint, role)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def traced_factory(real, endpoints: dict[str, str]):
    normalized = {value.rstrip("/"): key for key, value in endpoints.items()}

    def factory(*args, **kwargs):
        endpoint = str(kwargs.get("base_url", "unknown")).rstrip("/")
        if endpoint not in normalized:
            raise RuntimeError(f"unapproved model endpoint rejected: {endpoint}")
        return ClientProxy(real(*args, **kwargs), endpoint, normalized[endpoint])

    return factory


def verify_served_models(expected: dict[str, tuple[str, str]]) -> dict[str, list[str]]:
    observed = {}
    for role, (base_url, model_name) in expected.items():
        with urllib.request.urlopen(base_url.rstrip("/") + "/models", timeout=10) as response:
            ids = [str(row["id"]) for row in json.load(response).get("data", [])]
        if model_name not in ids:
            raise RuntimeError(f"{role} expected model {model_name!r}, observed {ids!r}")
        observed[role] = ids
    return observed


def find_db(root: str, db_id: str) -> Path:
    for path in (Path(root) / db_id / f"{db_id}.sqlite", Path(root) / db_id / f"{db_id}.db"):
        if path.is_file():
            return path
    raise FileNotFoundError(f"database file absent for {db_id}")


def execute_readonly(sql: str, db_path: Path, timeout: float) -> list[tuple]:
    if not sql.strip():
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


def evaluate_ex(pred: str, gold: str, db_path: Path, timeout: float = 30) -> tuple[int, dict | None, str | None]:
    """BIRD-compatible set(rows) EX; gold/DB failure is infrastructure."""
    try:
        gold_rows = execute_readonly(gold, db_path, timeout)
    except Exception as exc:
        return 0, safe_error(exc), "gold_or_database"
    try:
        pred_rows = execute_readonly(pred, db_path, timeout)
        return int(set(pred_rows) == set(gold_rows)), None, None
    except Exception as exc:
        return 0, safe_error(exc), "predicted_sql"


def install_readonly_agent_sql_check(module) -> None:
    """Prevent a generated statement from mutating the shared benchmark DB."""
    def check(self, unmasked_sql: str, db_path: str) -> str:
        if not unmasked_sql or not db_path:
            return "Error: Missing SQL or Database Path."
        try:
            execute_readonly(unmasked_sql, Path(db_path), 15)
            return "Success"
        except sqlite3.OperationalError as exc:
            if "interrupted" in str(exc).lower():
                return "SQL Execution Error: Execution timed out (exceeded 15 seconds)."
            return f"SQL Execution Error: {exc}"
        except Exception as exc:
            return f"SQL Execution Error: {exc}"
    module.PrivacyProtectionAgent._tool_execute_sql_check = check


def is_budget_failure(call: dict) -> bool:
    if call.get("status_code") != 400:
        return False
    text = call.get("error_summary", "").lower()
    return any(term in text for term in ("context length", "context_length", "maximum context", "max_tokens", "token limit"))


def request_costs(trace: list[dict]) -> dict[str, int]:
    roles = ("agent", "query_rewrite", "entity_extract", "sql_generator")
    totals = {role: 0 for role in roles}
    for request in trace:
        usage = request.get("usage") or {}
        totals.setdefault(request.get("role", "unknown"), 0)
        totals[request.get("role", "unknown")] += int(usage.get("total_tokens") or 0)
    totals["all"] = sum(value for key, value in totals.items() if key != "all")
    return totals


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("model", choices=("sft", "rl"))
    parser.add_argument("run", type=int, choices=(1, 2, 3))
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--execute-reviewed", action="store_true")
    args = parser.parse_args()
    if not args.execute_reviewed:
        raise SystemExit("pass --execute-reviewed after reviewing preflight_report.json")

    manifest_bytes = (HERE / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    preflight = json.loads((HERE / "preflight_report.json").read_text())
    if preflight.get("status") != "ready":
        raise SystemExit("preflight is not ready")
    if preflight.get("artifacts", {}).get("manifest_sha256") != hashlib.sha256(manifest_bytes).hexdigest():
        raise SystemExit("manifest changed after preflight; rerun preflight.py")

    output = Path(args.output)
    if output.exists() and any(output.iterdir()):
        raise SystemExit("output directory must be fresh and empty; partial repetitions are never resumed")
    output.mkdir(parents=True, exist_ok=True)
    (output / "case_logs").mkdir()

    inf = manifest["inference"]
    tools = inf["tools_checkpoint"]
    generator = inf["cloud_generator"]  # Historical schema name; endpoint is locked to loopback below.
    if not re.fullmatch(r"https?://(?:127\.0\.0\.1|localhost):\d+(?:/v1)?/?", generator["base_url"]):
        raise SystemExit("SQL generator must be an explicitly configured loopback endpoint")
    endpoints = {
        "agent": manifest["models"][args.model]["base_url"],
        "query_rewrite": tools["query_rewrite_base_url"],
        "entity_extract": tools["entity_extract_base_url"],
        "sql_generator": generator["base_url"],
    }
    served_models = verify_served_models({
        "agent": (endpoints["agent"], manifest["models"][args.model]["served_model_name"]),
        "query_rewrite": (endpoints["query_rewrite"], tools["query_rewrite_model"]),
        "entity_extract": (endpoints["entity_extract"], tools["entity_extract_model"]),
        "sql_generator": (endpoints["sql_generator"], generator["model_name"]),
    })
    os.environ.update(
        QUERY_REWRITE_BASE_URL=tools["query_rewrite_base_url"],
        QUERY_REWRITE_MODEL_NAME=tools["query_rewrite_model"],
        ENTITY_EXTRACT_BASE_URL=tools["entity_extract_base_url"],
        ENTITY_EXTRACT_MODEL_NAME=tools["entity_extract_model"],
        SQL_GENERATE_API_KEY="EMPTY",
        SQL_GENERATE_BASE_URL=generator["base_url"],
        SQL_GENERATE_MODEL_NAME=generator["model_name"],
        QUERY_REWRITE_MAX_TOKENS=str(inf["query_rewrite_max_tokens"]),
        ENTITY_EXTRACT_MAX_TOKENS=str(inf["entity_extract_max_tokens"]),
        SQL_GENERATE_MAX_TOKENS=str(inf["sql_generate_max_tokens"]),
        AGENT_MAX_TOKENS=str(inf["agent_max_tokens"]),
    )

    module = load_legacy()
    module.OpenAI = traced_factory(module.OpenAI, endpoints)
    install_readonly_agent_sql_check(module)
    data = module.read_data(manifest["dataset"]["source"])
    data, match_report = module.filter_and_attach_masksql_annotations(
        data, manifest["dataset"]["masksql_annotations"], manifest["dataset"]["match_mode"]
    )
    if len(data) != manifest["dataset"]["expected_cases"]:
        raise RuntimeError(f"matched {len(data)}, expected {manifest['dataset']['expected_cases']}")
    canonical_ids = [str(row["idx"]) for row in json.loads(Path(manifest["dataset"]["masksql_annotations"]).read_text())]
    by_id = {str(item["masksql_idx"]): item for item in data}
    if set(by_id) != set(canonical_ids) or len(by_id) != len(data):
        raise RuntimeError("matched data IDs differ from canonical annotation idx values")
    selected_ids = canonical_ids[: args.limit or None]
    selected = [by_id[question_id] for question_id in selected_ids]

    locked_config = {
        "dataset": manifest["dataset"], "evaluator": manifest["evaluator"],
        "inference": manifest["inference"], "policy": manifest["policy"],
        "repetitions": manifest["repetitions"],
    }
    metadata = {
        "status": "running", "model": args.model, "run": args.run,
        "config_hash": canonical_hash(locked_config),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "model_identity": preflight["models"][args.model],
        "fresh_output_dir": True, "workers": args.workers, "limit": args.limit,
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "legacy_evaluator_sha256": hashlib.sha256(LEGACY.read_bytes()).hexdigest(),
        "prompt_template_sha256": {
            name: hashlib.sha256((TEMPLATE_DIR / name).read_bytes()).hexdigest()
            for name in ("query_rewrite_template.py", "entity_extract_template.py", "sql_generate_template.py")
        },
        "served_models_pre_run": served_models,
        "question_ids": selected_ids, "match_report": match_report,
        "started_unix": time.time(),
    }
    write_json(output / "metadata.json", metadata)

    agent = module.PrivacyProtectionAgent(
        endpoints["agent"], manifest["models"][args.model]["served_model_name"],
        db_base_dir=manifest["dataset"]["runtime_db_root"], log_dir=str(output / "case_logs")
    )
    cases_path = output / "cases.jsonl"

    def one(pair):
        index, item = pair
        question_id = str(item["masksql_idx"])
        TLS.trace = []
        db_error = None
        try:
            db_path = find_db(manifest["dataset"]["runtime_db_root"], item["db_id"])
            # Gold is checked before costly model calls so evaluator breakage is never scored as zero.
            execute_readonly(item["gold_sql"], db_path, 30)
        except Exception as exc:
            db_error = safe_error(exc)
        if db_error:
            return {
                "question_id": question_id, "case_id": question_id, "execution_correct": 0,
                "task_status": "infrastructure_error", "failure_kind": "gold_or_database",
                "trace_complete": True, "all_model_requests": [], "cloud_requests": [],
                "cost_tokens_by_role": request_costs([]), "cost_tokens": 0, "execution_error": db_error,
                "db_id": item["db_id"], "gold_sql": item["gold_sql"], "pred_sql": "",
                "annotated_links": item.get("annotated_links", {}), "masked_terms": [],
            }
        result = module.process_single_item(index, item, agent, len(selected))
        trace = list(TLS.trace)
        ex, ex_error, ex_failure = evaluate_ex(result["pred_sql"], result["gold_sql"], db_path)
        failed_calls = [call for call in trace if not call.get("ok")]
        timeout = bool((result.get("tool_use_audit") or {}).get("timed_out"))
        if failed_calls and not all(is_budget_failure(call) for call in failed_calls):
            task_status, failure_kind = "infrastructure_error", "model_api"
        elif timeout:
            task_status, failure_kind = "valid", "agent_turn_limit"
        elif failed_calls:
            task_status, failure_kind = "valid", "model_budget"
        elif ex_failure == "gold_or_database":
            task_status, failure_kind = "infrastructure_error", ex_failure
        elif ex_failure:
            task_status, failure_kind = "valid", ex_failure
        else:
            task_status, failure_kind = "valid", None
        cloud = [request for request in trace if request.get("role") == "sql_generator"]
        return {
            "question_id": question_id, "case_id": question_id, "execution_correct": ex,
            "task_status": task_status, "failure_kind": failure_kind, "trace_complete": True,
            "all_model_requests": trace, "cloud_requests": cloud,
            "cost_tokens_by_role": request_costs(trace), "cost_tokens": request_costs(trace)["all"],
            "execution_error": ex_error, "db_id": result["db_id"],
            "pred_sql": result["pred_sql"], "gold_sql": result["gold_sql"],
            "annotated_links": result["annotated_links"], "masked_terms": result["masked_terms"],
            "tool_use_audit": result.get("tool_use_audit"),
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
