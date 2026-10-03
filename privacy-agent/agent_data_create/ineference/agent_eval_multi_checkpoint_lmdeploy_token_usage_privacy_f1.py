
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))

import os
import json
import logging
from openai import OpenAI
from typing import Dict, List, Any, Tuple
import re
import copy
import subprocess
import time
import traceback
import sqlite3
import argparse
import glob
import shlex
import signal
from dataclasses import dataclass, asdict
from pathlib import Path
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed 
from statistics import mean
from tools import generate_schema_string_from_masked, execute_sql,  find_result, safe_execute_sql
from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template

# Uncomment if you need specific dependencies
# from transformers import AutoModelForCausalLM, AutoTokenizer
# import sqlglot
# import sqlite3
import json_repair  

# Import your existing template files
from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template
from tools import generate_schema_string_from_masked

# NOTE: tokenizer/model loading is intentionally removed from the evaluator.
# Evaluation only talks to OpenAI-compatible inference servers, so loading a
# tokenizer here wastes memory and slows down multi-checkpoint runs.

# Configure logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Shared SQL-generation / RI-attack API defaults.
# If run_ri_attack uses the same model as sql_generate, both modules read from
# these same environment variables.
SQL_GENERATE_DEFAULT_BASE_URL = "https://api.deepseek.com/v1"
SQL_GENERATE_DEFAULT_MODEL_NAME = "deepseek-chat"
SQL_GENERATE_DEFAULT_API_KEY = "EMPTY"


def get_sql_generate_api_config() -> Dict[str, str]:
    return {
        "api_key": os.getenv("SQL_GENERATE_API_KEY", SQL_GENERATE_DEFAULT_API_KEY),
        "base_url": os.getenv("SQL_GENERATE_BASE_URL", SQL_GENERATE_DEFAULT_BASE_URL),
        "model_name": os.getenv("SQL_GENERATE_MODEL_NAME", SQL_GENERATE_DEFAULT_MODEL_NAME),
    }


def get_agent_max_tokens() -> int:
    try:
        return max(int(os.getenv("AGENT_MAX_TOKENS", "2048")), 1)
    except Exception:
        return 2048


def get_env_max_tokens(name: str, default: int) -> int:
    try:
        return max(int(os.getenv(name, str(default))), 1)
    except Exception:
        return default

# ==========================================
# Token usage utilities
# ==========================================
# Token usage for comparison with MaskSQL-style "Tokens" should reflect the
# model-call cost needed to produce one SQL prediction. We include all inference
# LLM calls used by the agent pipeline before the final SQL prediction:
#   - agent: local checkpoint/controller ReAct calls
#   - query_rewrite: local/trusted rewrite model tool call
#   - entity_extract: local/trusted entity extraction model tool call
#   - sql_generate: remote SQL-generation model call
# Pure Python tools such as apply_typed_masking/sql_unmask/execute_sql_check do
# not consume LM tokens and are not counted. RI attack is an evaluation-only
# procedure and is reported separately in privacy_eval.json, not in the main
# SQL-generation token usage metric.
TOKEN_USAGE_COMPONENTS = [
    "agent",
    "query_rewrite",
    "entity_extract",
    "sql_generate",
]


def _empty_usage() -> Dict[str, int]:
    return {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "num_calls": 0,
        "num_missing_usage": 0,
    }


def _new_token_usage_container() -> Dict[str, Dict[str, int]]:
    usage = {component: _empty_usage() for component in TOKEN_USAGE_COMPONENTS}
    usage["sql_generation_total"] = _empty_usage()
    usage["all_total"] = _empty_usage()
    return usage


def _usage_to_dict(response: Any) -> Dict[str, int]:
    """Extract OpenAI-compatible token usage from a response object.

    Works for OpenAI SDK objects and dict-like responses. If the serving backend
    does not return usage, returns zeros with num_missing_usage=1.
    """
    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, dict):
        usage = response.get("usage")

    if usage is None:
        missing = _empty_usage()
        missing["num_missing_usage"] = 1
        return missing

    def _get(name: str, default: int = 0) -> int:
        if isinstance(usage, dict):
            value = usage.get(name, default)
        else:
            value = getattr(usage, name, default)
        try:
            return int(value or 0)
        except Exception:
            return default

    prompt_tokens = _get("prompt_tokens")
    completion_tokens = _get("completion_tokens")
    total_tokens = _get("total_tokens")
    if total_tokens == 0:
        total_tokens = prompt_tokens + completion_tokens

    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "num_calls": 1,
        "num_missing_usage": 0,
    }


def _add_usage(dst: Dict[str, int], src: Dict[str, int]) -> None:
    for key in ["prompt_tokens", "completion_tokens", "total_tokens", "num_calls", "num_missing_usage"]:
        dst[key] = int(dst.get(key, 0)) + int(src.get(key, 0))


def _record_response_usage(
    token_usage: Dict[str, Dict[str, int]],
    component: str,
    response: Any,
    include_in_sql_generation_total: bool = True,
) -> Dict[str, int]:
    """Record token usage for one model response."""
    if token_usage is None:
        return _usage_to_dict(response)

    if component not in token_usage:
        token_usage[component] = _empty_usage()

    usage = _usage_to_dict(response)
    _add_usage(token_usage[component], usage)
    _add_usage(token_usage["all_total"], usage)

    if include_in_sql_generation_total:
        _add_usage(token_usage["sql_generation_total"], usage)

    return usage


def _aggregate_token_usage(results_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate per-case token usage into MaskSQL-style average totals."""
    aggregate = {component: _empty_usage() for component in TOKEN_USAGE_COMPONENTS}
    aggregate["sql_generation_total"] = _empty_usage()
    aggregate["all_total"] = _empty_usage()

    num_cases = len(results_list)
    for res in results_list:
        usage = res.get("token_usage") or {}
        for component, stats in usage.items():
            if component not in aggregate:
                aggregate[component] = _empty_usage()
            if isinstance(stats, dict):
                _add_usage(aggregate[component], stats)

    def _with_avg(stats: Dict[str, int]) -> Dict[str, Any]:
        out = dict(stats)
        denom = max(num_cases, 1)
        out["avg_prompt_tokens_per_case"] = stats.get("prompt_tokens", 0) / denom
        out["avg_completion_tokens_per_case"] = stats.get("completion_tokens", 0) / denom
        out["avg_total_tokens_per_case"] = stats.get("total_tokens", 0) / denom
        out["avg_calls_per_case"] = stats.get("num_calls", 0) / denom
        return out

    by_component = {k: _with_avg(v) for k, v in aggregate.items()}

    return {
        "num_cases": num_cases,
        "definition": (
            "MaskSQL-style token usage: average tokens per SQL generation. "
            "In this evaluator, sql_generation_total includes all model calls "
            "used by the agent pipeline to produce the final SQL: agent, "
            "query_rewrite, entity_extract, and sql_generate. Pure local Python "
            "tools such as apply_typed_masking, sql_unmask, and execute_sql_check "
            "do not consume LM tokens and are not counted. RI attack is an "
            "evaluation-only procedure and is reported separately in privacy_eval.json."
        ),
        "average_total_tokens_per_sql_generation": by_component["sql_generation_total"]["avg_total_tokens_per_case"],
        "average_prompt_tokens_per_sql_generation": by_component["sql_generation_total"]["avg_prompt_tokens_per_case"],
        "average_completion_tokens_per_sql_generation": by_component["sql_generation_total"]["avg_completion_tokens_per_case"],
        "total_tokens_for_sql_generation": by_component["sql_generation_total"]["total_tokens"],
        "total_prompt_tokens_for_sql_generation": by_component["sql_generation_total"]["prompt_tokens"],
        "total_completion_tokens_for_sql_generation": by_component["sql_generation_total"]["completion_tokens"],
        "token_usage_scope": "agent_pipeline_model_calls_excluding_evaluation",
        "included_in_main_metric": ["agent", "query_rewrite", "entity_extract", "sql_generate"],
        "excluded_from_main_metric": ["apply_typed_masking", "sql_unmask", "execute_sql_check", "ri_attack"],
        "by_component": by_component,
    }

# ==========================================
# Utility functions: SQL cleaning and extraction
# ==========================================
def clean_space(sql):
    sql = re.sub(r'\s*\)', ')', sql)
    # Aggregate function brackets need to be connected
    sql = re.sub(r'min\s*\(\s*', 'min(', sql)
    sql = re.sub(r'max\s*\(\s*', 'max(', sql)
    sql = re.sub(r'sum\s*\(\s*', 'sum(', sql)
    sql = re.sub(r'avg\s*\(\s*', 'avg(', sql)
    sql = re.sub(r'count\s*\(\s*', 'count(', sql)
    sql = re.sub(r'MIN\s*\(\s*', 'MIN(', sql)
    sql = re.sub(r'MAX\s*\(\s*', 'MAX(', sql)
    sql = re.sub(r'SUM\s*\(\s*', 'SUM(', sql)
    sql = re.sub(r'AVG\s*\(\s*', 'AVG(', sql)
    sql = re.sub(r'COUNT\s*\(\s*', 'COUNT(', sql)
    # For non-aggregate functions, only delete spaces to the right of the left bracket
    sql = re.sub(r'\(\s*', '(', sql)
    # Delete extra spaces
    sql = re.sub(r' +', ' ', sql)
    sql = re.sub(r'\s*,', ',', sql) 
    return sql

def find_result(text, language='sql'):
    sql = text
    if '```sql' in text:
        pattern = r"```sql(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        if len(matches) > 0:
            sql = matches[-1].replace('\n', ' ').strip()
            sql = clean_space(sql)
            return sql
        
    sql = sql.replace('\n', ' ').strip()
    return sql

def read_data(path, encoding="utf-8"):
    # 1. Read data as a list based on different formats
    if path.endswith('.jsonl'):
        with open(path, "r", encoding=encoding) as f:
            data = [json.loads(line) for line in f if line.strip()]
    else:
        with open(path, "r", encoding=encoding) as f:
            data = json.load(f)
            
    # 2. Sort in ascending order based on question_id
    sorted_data = sorted(data, key=lambda x: int(x.get('question_id', 0)))
    

    return sorted_data


def _coerce_json_object(value: Any, default: Any = None, max_depth: int = 3) -> Any:
    """
    Robustly parse model-emitted tool-call payloads.

    Some local OpenAI-compatible servers return function arguments as a JSON
    string, and some models emit nested JSON strings such as:
      {"name": "query_rewrite", "arguments": "{\"query_to_rewrite\": \"...\"}"}
    The normal tool execution code expects a dict, so we recursively parse
    strings until we either get a structured object or give up.
    """
    if default is None:
        default = {}
    current = value
    for _ in range(max_depth):
        if isinstance(current, (dict, list)):
            return current
        if current is None:
            return default
        if isinstance(current, str):
            text = current.strip()
            if not text:
                return default
            try:
                current = json_repair.loads(text)
                continue
            except Exception:
                try:
                    current = json.loads(text)
                    continue
                except Exception:
                    return default
        return default
    return current if isinstance(current, (dict, list)) else default


def _coerce_tool_arguments(arguments: Any) -> Dict[str, Any]:
    """Return a dict for tool arguments even when the model emitted malformed args."""
    parsed = _coerce_json_object(arguments, default={})
    if isinstance(parsed, dict):
        return parsed
    # Do not crash if the model mistakenly emits a list or scalar as arguments.
    return {"_raw_arguments": parsed}


def _normalize_tool_call_payload(tool_data: Any) -> Tuple[str, Dict[str, Any]]:
    """Normalize multiple possible tool-call JSON shapes into (name, args)."""
    tool_obj = _coerce_json_object(tool_data, default={})
    if not isinstance(tool_obj, dict):
        return "", {}

    # Common custom shape: {"name": ..., "arguments": {...}}
    name = tool_obj.get("name") or tool_obj.get("tool") or tool_obj.get("function_name") or ""
    arguments = tool_obj.get("arguments", {})

    # OpenAI native-like shape: {"function": {"name": ..., "arguments": ...}}
    function_obj = tool_obj.get("function")
    if isinstance(function_obj, dict):
        name = function_obj.get("name", name)
        arguments = function_obj.get("arguments", arguments)

    return str(name).strip(), _coerce_tool_arguments(arguments)


def _tool_schema_specs(tools: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Extract required/allowed argument names from OpenAI tool schemas."""
    specs = {}
    for tool in tools:
        fn = (tool or {}).get("function") or {}
        name = str(fn.get("name") or "").strip()
        params = fn.get("parameters") or {}
        properties = params.get("properties") or {}
        if not name:
            continue
        specs[name] = {
            "required": set(params.get("required") or []),
            "allowed": set(properties.keys()),
        }
    return specs


def _new_tool_audit() -> Dict[str, Any]:
    return {
        "turns": [],
        "num_model_turns": 0,
        "num_tool_calls": 0,
        "num_parse_errors": 0,
        "num_missing_tool_calls": 0,
        "num_valid_function_name": 0,
        "num_invalid_function_name": 0,
        "num_valid_arguments": 0,
        "num_invalid_arguments": 0,
        "num_missing_required_arguments": 0,
        "num_unexpected_arguments": 0,
        "num_tool_execution_errors": 0,
        "called_tools": [],
        "invalid_function_names": [],
        "missing_required_arguments": [],
        "unexpected_arguments": [],
        "completed_with_final_answer": False,
        "timed_out": False,
    }


def _record_no_tool_turn(audit: Dict[str, Any], iteration: int, content: str) -> None:
    audit["num_model_turns"] += 1
    audit["num_missing_tool_calls"] += 1
    audit["turns"].append({
        "iteration": iteration + 1,
        "has_tool_call": False,
        "content_preview": str(content or "")[:500],
    })


_REGISTERED_TOOL_NAMES = (
    "query_rewrite",
    "entity_extract",
    "apply_typed_masking",
    "sql_generate",
    "sql_unmask",
    "execute_sql_check",
)


def _extract_textual_action(content: str) -> Tuple[str, Dict[str, Any]]:
    """Best-effort parser for ReAct-style text actions that are not real tool calls."""
    text = str(content or "")
    match = re.search(r"Action\s*:\s*`?([A-Za-z_][A-Za-z0-9_]*)`?\s*(?:\((.*?)\))?", text, re.DOTALL)
    if not match:
        tool_names = "|".join(re.escape(name) for name in _REGISTERED_TOOL_NAMES)
        match = re.search(rf"\b(?:call|use|invoke|trigger)\s+`?({tool_names})`?\b", text, re.IGNORECASE)
    if not match:
        lowered = text.lower()
        intent_patterns = [
            ("query_rewrite", [
                r"\b(?:let me|i will|i should|proceed to|need to|going to)\s+rewrite (?:the )?quer",
            ]),
            ("entity_extract", [
                r"\b(?:let me|i will|i should|proceed to|need to|going to|start by|first)\s+(?:extract|perform entity extraction)",
                r"\b(?:let me|i will|i should|proceed to|need to|going to)\s+start (?:with|by) entity extraction",
            ]),
            ("apply_typed_masking", [
                r"\b(?:let me|i will|i should|proceed to|need to|going to)\s+(?:apply|perform|do) (?:typed )?masking",
                r"\b(?:let me|i will|i should|proceed to|need to|going to)\s+mask (?:the )?(?:query|schema|entities)",
            ]),
            ("sql_generate", [
                r"\b(?:let me|i will|i should|proceed to|need to|going to)\s+generate (?:the )?(?:masked |desensitized )?sql",
            ]),
            ("sql_unmask", [
                r"\b(?:let me|i will|i should|proceed to|need to|going to)\s+(?:unmask|restore) (?:the )?(?:real )?sql",
            ]),
            ("execute_sql_check", [
                r"\b(?:let me|i will|i should|proceed to|need to|going to)\s+(?:execute|verify|check) (?:the )?(?:sql|execution)",
            ]),
        ]
        for tool_name, patterns in intent_patterns:
            if any(re.search(pattern, lowered) for pattern in patterns):
                return tool_name, {}
    if not match:
        return "", {}
    name = match.group(1).strip()
    raw_args = (match.group(2) if len(match.groups()) > 1 else "") or ""
    raw_args = raw_args.strip()
    if name not in _REGISTERED_TOOL_NAMES:
        # Do not count generic ReAct verbs such as "Action: call ..." as bad
        # function names. They are protocol wrappers, not attempted tool names.
        if name.lower() in {"call", "use", "invoke", "trigger"}:
            tool_names = "|".join(re.escape(tool_name) for tool_name in _REGISTERED_TOOL_NAMES)
            followup = re.search(rf"\b{name}\s+`?({tool_names})`?\b", text, re.IGNORECASE)
            return (followup.group(1), {}) if followup else ("", {})
    if not raw_args:
        return name, {}

    parsed = _coerce_json_object(raw_args, default=None)
    if isinstance(parsed, dict):
        return name, parsed

    args: Dict[str, Any] = {}
    for key in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*=", raw_args):
        args[key] = "<textual_action_value>"
    if not args:
        args["_raw_arguments"] = raw_args[:500]
    return name, args


def _extract_sql_text(text: str) -> str:
    """Extract the most likely SQL string from a tool/model response."""
    text = str(text or "").strip()
    if not text:
        return ""
    fenced = re.search(r"```(?:sql)?\s*(.*?)\s*```", text, re.IGNORECASE | re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    select_match = re.search(r"\b(?:SELECT|WITH)\b[\s\S]*", text, re.IGNORECASE)
    if select_match:
        candidate = select_match.group(0).strip()
        # Stop before common explanatory text that may follow the SQL.
        candidate = re.split(r"\n\s*(?:Observation|Thought|Action)\s*:", candidate, maxsplit=1)[0].strip()
        return candidate
    return text


def _coerce_entity_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    if isinstance(value, str):
        try:
            parsed = json_repair.loads(value)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict)]
        except Exception:
            return []
    return []


def _record_tool_parse_error(audit: Dict[str, Any], iteration: int, raw_payload: str, error: Exception) -> None:
    audit["num_model_turns"] += 1
    audit["num_parse_errors"] += 1
    audit["turns"].append({
        "iteration": iteration + 1,
        "has_tool_call": True,
        "parse_error": f"{type(error).__name__}: {error}",
        "raw_payload_preview": str(raw_payload or "")[:500],
    })


def _audit_tool_call(
    audit: Dict[str, Any],
    iteration: int,
    func_name: str,
    args: Dict[str, Any],
    tool_specs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    spec = tool_specs.get(func_name)
    args = args if isinstance(args, dict) else {"_raw_arguments": args}
    arg_keys = set(args.keys())
    required = spec["required"] if spec else set()
    allowed = spec["allowed"] if spec else set()
    missing_required = sorted(required - arg_keys)
    unexpected = sorted(arg_keys - allowed) if spec else sorted(arg_keys)
    valid_function_name = spec is not None
    valid_arguments = bool(valid_function_name and not missing_required)

    audit["num_model_turns"] += 1
    audit["num_tool_calls"] += 1
    audit["called_tools"].append(func_name)
    if valid_function_name:
        audit["num_valid_function_name"] += 1
    else:
        audit["num_invalid_function_name"] += 1
        audit["invalid_function_names"].append(func_name)
    if valid_arguments:
        audit["num_valid_arguments"] += 1
    else:
        audit["num_invalid_arguments"] += 1
    if missing_required:
        audit["num_missing_required_arguments"] += 1
        audit["missing_required_arguments"].append({
            "iteration": iteration + 1,
            "tool": func_name,
            "missing": missing_required,
        })
    if unexpected:
        audit["num_unexpected_arguments"] += 1
        audit["unexpected_arguments"].append({
            "iteration": iteration + 1,
            "tool": func_name,
            "unexpected": unexpected,
        })

    turn_record = {
        "iteration": iteration + 1,
        "has_tool_call": True,
        "function_name": func_name,
        "argument_keys": sorted(arg_keys),
        "valid_function_name": valid_function_name,
        "valid_arguments": valid_arguments,
        "missing_required_arguments": missing_required,
        "unexpected_arguments": unexpected,
        "tool_execution_error": False,
    }
    audit["turns"].append(turn_record)
    return turn_record


def _record_textual_action_audit(
    audit: Dict[str, Any],
    iteration: int,
    content: str,
    tool_specs: Dict[str, Dict[str, Any]],
) -> bool:
    func_name, args = _extract_textual_action(content)
    if not func_name:
        return False
    turn = _audit_tool_call(audit, iteration, func_name, args, tool_specs)
    turn["has_tool_call"] = False
    turn["textual_action_only"] = True
    turn["content_preview"] = str(content or "")[:500]
    audit["num_parse_errors"] += 1
    return True


def _finalize_tool_audit(audit: Dict[str, Any], pred_sql: str = "") -> Dict[str, Any]:
    strict_pipeline = [
        "entity_extract",
        "apply_typed_masking",
        "sql_generate",
        "sql_unmask",
        "execute_sql_check",
    ]
    core_pipeline = [
        "entity_extract",
        "apply_typed_masking",
    ]
    called = set(audit.get("called_tools") or [])
    audit["has_final_sql"] = bool(str(pred_sql or "").strip())
    audit["strict_pipeline_covered"] = all(name in called for name in strict_pipeline)
    audit["core_pipeline_covered"] = (
        all(name in called for name in core_pipeline)
        and ("sql_generate" in called or audit["has_final_sql"])
    )
    # Reviewer-facing full-chain coverage: the agent follows the privacy tool
    # workflow far enough to produce a recoverable final SQL. Strict execution
    # coverage is kept separately because sql_unmask/execute_sql_check are
    # optional repair/checking turns in many successful traces.
    audit["pipeline_covered"] = audit["core_pipeline_covered"]
    audit["malformed_protocol"] = any(
        (turn.get("valid_function_name") is False or turn.get("valid_arguments") is False)
        for turn in audit.get("turns") or []
        if turn.get("function_name") is not None
    )
    audit["tool_call_valid_rate"] = (
        audit["num_valid_function_name"] / audit["num_tool_calls"]
        if audit["num_tool_calls"] else 0.0
    )
    audit["argument_valid_rate"] = (
        audit["num_valid_arguments"] / audit["num_tool_calls"]
        if audit["num_tool_calls"] else 0.0
    )
    audit["tool_execution_error_rate"] = (
        audit["num_tool_execution_errors"] / audit["num_tool_calls"]
        if audit["num_tool_calls"] else 0.0
    )
    audit["task_completed_by_format"] = bool(
        audit.get("completed_with_final_answer")
        and audit.get("pipeline_covered")
        and audit.get("has_final_sql")
    )
    return audit


def aggregate_tool_use_audit(results_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    audits = [row.get("tool_use_audit") or {} for row in results_list]
    n = len(audits)

    def _sum(key: str) -> int:
        return sum(int(a.get(key, 0) or 0) for a in audits)

    def _case_rate(key: str) -> float:
        return sum(1 for a in audits if a.get(key)) / n if n else 0.0

    def _macro_avg(key: str) -> float:
        return sum(float(a.get(key, 0.0) or 0.0) for a in audits) / n if n else 0.0

    total_tool_calls = _sum("num_tool_calls")
    total_model_turns = _sum("num_model_turns")
    total_valid_function = _sum("num_valid_function_name")
    total_valid_arguments = _sum("num_valid_arguments")
    total_invalid_function = _sum("num_invalid_function_name")
    total_invalid_arguments = _sum("num_invalid_arguments")

    tool_counter: Dict[str, int] = {}
    invalid_name_counter: Dict[str, int] = {}
    missing_arg_counter: Dict[str, int] = {}
    unexpected_arg_counter: Dict[str, int] = {}
    malformed_turns = 0
    for audit in audits:
        for name in audit.get("called_tools") or []:
            tool_counter[name] = tool_counter.get(name, 0) + 1
        for turn in audit.get("turns") or []:
            if turn.get("function_name") is None:
                continue
            if turn.get("valid_function_name") is False or turn.get("valid_arguments") is False:
                malformed_turns += 1
        for name in audit.get("invalid_function_names") or []:
            invalid_name_counter[name] = invalid_name_counter.get(name, 0) + 1
        for item in audit.get("missing_required_arguments") or []:
            tool = item.get("tool", "")
            for arg in item.get("missing") or []:
                key = f"{tool}.{arg}"
                missing_arg_counter[key] = missing_arg_counter.get(key, 0) + 1
        for item in audit.get("unexpected_arguments") or []:
            tool = item.get("tool", "")
            for arg in item.get("unexpected") or []:
                key = f"{tool}.{arg}"
                unexpected_arg_counter[key] = unexpected_arg_counter.get(key, 0) + 1

    return {
        "num_cases": n,
        "total_model_turns": total_model_turns,
        "total_tool_calls": total_tool_calls,
        "avg_tool_calls_per_case": total_tool_calls / n if n else 0.0,
        "function_name_accuracy": total_valid_function / total_tool_calls if total_tool_calls else 0.0,
        "argument_validity_rate": total_valid_arguments / total_tool_calls if total_tool_calls else 0.0,
        "macro_function_name_accuracy": _macro_avg("tool_call_valid_rate"),
        "macro_argument_validity_rate": _macro_avg("argument_valid_rate"),
        "invalid_function_name_rate": total_invalid_function / total_tool_calls if total_tool_calls else 0.0,
        "invalid_argument_rate": total_invalid_arguments / total_tool_calls if total_tool_calls else 0.0,
        "malformed_protocol_rate": malformed_turns / total_tool_calls if total_tool_calls else 0.0,
        "parse_error_rate_per_turn": _sum("num_parse_errors") / total_model_turns if total_model_turns else 0.0,
        "missing_tool_call_rate_per_turn": _sum("num_missing_tool_calls") / total_model_turns if total_model_turns else 0.0,
        "tool_execution_error_rate": _sum("num_tool_execution_errors") / total_tool_calls if total_tool_calls else 0.0,
        "case_pipeline_covered_rate": _case_rate("pipeline_covered"),
        "case_strict_pipeline_covered_rate": _case_rate("strict_pipeline_covered"),
        "case_core_pipeline_covered_rate": _case_rate("core_pipeline_covered"),
        "case_completed_by_format_rate": _case_rate("task_completed_by_format"),
        "case_has_final_sql_rate": _case_rate("has_final_sql"),
        "case_no_invalid_function_name_rate": sum(
            1 for a in audits if int(a.get("num_invalid_function_name", 0) or 0) == 0
        ) / n if n else 0.0,
        "case_no_invalid_argument_rate": sum(
            1 for a in audits if int(a.get("num_invalid_arguments", 0) or 0) == 0
        ) / n if n else 0.0,
        "case_no_malformed_protocol_rate": sum(
            1 for a in audits if not a.get("malformed_protocol")
        ) / n if n else 0.0,
        "case_timeout_rate": _case_rate("timed_out"),
        "counts": {
            "valid_function_name": total_valid_function,
            "invalid_function_name": total_invalid_function,
            "valid_arguments": total_valid_arguments,
            "invalid_arguments": total_invalid_arguments,
            "malformed_turns": malformed_turns,
            "parse_errors": _sum("num_parse_errors"),
            "missing_required_arguments": _sum("num_missing_required_arguments"),
            "unexpected_arguments": _sum("num_unexpected_arguments"),
            "tool_execution_errors": _sum("num_tool_execution_errors"),
        },
        "tool_call_counts": dict(sorted(tool_counter.items())),
        "invalid_function_name_counts": dict(sorted(invalid_name_counter.items())),
        "missing_required_argument_counts": dict(sorted(missing_arg_counter.items())),
        "unexpected_argument_counts": dict(sorted(unexpected_arg_counter.items())),
    }


def write_tool_use_audit_reports(report: Dict[str, Any], output_json: str, output_csv: str, output_md: str) -> None:
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    scalar_keys = [
        "num_cases",
        "total_model_turns",
        "total_tool_calls",
        "avg_tool_calls_per_case",
        "function_name_accuracy",
        "argument_validity_rate",
        "macro_function_name_accuracy",
        "macro_argument_validity_rate",
        "invalid_function_name_rate",
        "invalid_argument_rate",
        "malformed_protocol_rate",
        "parse_error_rate_per_turn",
        "missing_tool_call_rate_per_turn",
        "tool_execution_error_rate",
        "case_pipeline_covered_rate",
        "case_strict_pipeline_covered_rate",
        "case_core_pipeline_covered_rate",
        "case_completed_by_format_rate",
        "case_has_final_sql_rate",
        "case_no_invalid_function_name_rate",
        "case_no_invalid_argument_rate",
        "case_no_malformed_protocol_rate",
        "case_timeout_rate",
    ]
    with open(output_csv, "w", encoding="utf-8") as f:
        f.write("metric,value\n")
        for key in scalar_keys:
            f.write(f"{key},{report.get(key, '')}\n")
        for group_name in [
            "tool_call_counts",
            "invalid_function_name_counts",
            "missing_required_argument_counts",
            "unexpected_argument_counts",
        ]:
            for name, value in (report.get(group_name) or {}).items():
                safe_name = str(name).replace(",", ";")
                f.write(f"{group_name}.{safe_name},{value}\n")

    def pct(key: str) -> str:
        return f"{float(report.get(key, 0.0) or 0.0) * 100:.2f}"

    rows = [
        ("Cases", str(report.get("num_cases", 0))),
        ("Tool calls / case", f"{float(report.get('avg_tool_calls_per_case', 0.0) or 0.0):.2f}"),
        ("Function-name accuracy", pct("function_name_accuracy")),
        ("Argument validity", pct("argument_validity_rate")),
        ("Macro function-name accuracy", pct("macro_function_name_accuracy")),
        ("Macro argument validity", pct("macro_argument_validity_rate")),
        ("Invalid function-name rate", pct("invalid_function_name_rate")),
        ("Invalid argument rate", pct("invalid_argument_rate")),
        ("Malformed protocol rate", pct("malformed_protocol_rate")),
        ("Raw text-action parse rate / turn", pct("parse_error_rate_per_turn")),
        ("Tool execution-error rate", pct("tool_execution_error_rate")),
        ("Full tool-chain covered", pct("case_pipeline_covered_rate")),
        ("Strict pipeline covered", pct("case_strict_pipeline_covered_rate")),
        ("Completed by format", pct("case_completed_by_format_rate")),
        ("Has final SQL", pct("case_has_final_sql_rate")),
        ("Cases w/o invalid function name", pct("case_no_invalid_function_name_rate")),
        ("Cases w/o invalid arguments", pct("case_no_invalid_argument_rate")),
        ("Cases w/o malformed protocol", pct("case_no_malformed_protocol_rate")),
        ("Timeout", pct("case_timeout_rate")),
    ]
    with open(output_md, "w", encoding="utf-8") as f:
        f.write("| Metric | Value |\n")
        f.write("|---|---:|\n")
        for name, value in rows:
            suffix = "%" if name not in {"Cases", "Tool calls / case"} else ""
            f.write(f"| {name} | {value}{suffix} |\n")
        if report.get("invalid_function_name_counts"):
            f.write("\nInvalid function names:\n\n")
            for name, value in report["invalid_function_name_counts"].items():
                f.write(f"- `{name}`: {value}\n")
        if report.get("missing_required_argument_counts"):
            f.write("\nMissing required arguments:\n\n")
            for name, value in report["missing_required_argument_counts"].items():
                f.write(f"- `{name}`: {value}\n")

# ==========================================
# MaskSQL privacy annotation matching and metrics
# ==========================================
def _norm_text_for_match(value: Any) -> str:
    """Normalize text fields for robust dataset matching."""
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").strip()
    return re.sub(r"\s+", " ", text)


def _norm_sql_for_match(sql: Any) -> str:
    """Normalize SQL enough for identifying the same BIRD example."""
    text = _norm_text_for_match(sql).rstrip(";")
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\s*,\s*", ",", text)
    text = re.sub(r"\s+", " ", text)
    return text.lower()


def _bird_match_key(item: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        _norm_text_for_match(item.get("db_id")),
        _norm_text_for_match(item.get("question")),
        _norm_text_for_match(item.get("evidence")),
        _norm_sql_for_match(item.get("gold_sql")),
    )


def _masksql_match_key(item: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        _norm_text_for_match(item.get("orig_db_id")),
        _norm_text_for_match(item.get("orig_question")),
        _norm_text_for_match(item.get("orig_evidence")),
        _norm_sql_for_match(item.get("orig_gold_sql")),
    )


def _safe_question_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _masksql_question_id(item: Dict[str, Any]) -> str:
    idx = str(item.get("idx", ""))
    if idx.startswith("bird_"):
        return idx.split("_", 1)[1]
    return idx


def _combine_gold_links(masksql_row: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Return one annotation dictionary keyed by sensitive NL surface term."""
    combined: Dict[str, Dict[str, str]] = {}
    for term, target in (masksql_row.get("orig_gold_schema_links") or {}).items():
        combined[str(term)] = {"type": "schema", "target": str(target)}
    for term, target in (masksql_row.get("orig_gold_value_links") or {}).items():
        if str(term) in combined:
            combined[str(term)] = {
                "type": combined[str(term)]["type"] + "+value",
                "target": combined[str(term)]["target"] + " || " + str(target),
            }
        else:
            combined[str(term)] = {"type": "value", "target": str(target)}
    return combined


def load_masksql_test(masksql_test_path: str) -> List[Dict[str, Any]]:
    with open(masksql_test_path, "r", encoding="utf-8") as f:
        return json.load(f)


def filter_and_attach_masksql_annotations(
    data: List[Dict[str, Any]],
    masksql_test_path: str,
    match_mode: str = "auto",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Select BIRD rows that have MaskSQL human annotations and attach annotation fields.

    match_mode:
      - strict: use (db_id, question, evidence, gold_sql) only.
      - relaxed: strict first, then ignore evidence if the other fields match.
      - id: strict first, then use MaskSQL idx=bird_{question_id} fallback.
      - auto: strict -> relaxed -> id. This is useful when local BIRD files have
              evidence or SQL formatting drift from MaskSQL's released annotations.
    """
    mask_rows = load_masksql_test(masksql_test_path)
    strict_index = {_masksql_match_key(row): row for row in mask_rows}
    relaxed_index = {
        (_masksql_match_key(row)[0], _masksql_match_key(row)[1], _masksql_match_key(row)[3]): row
        for row in mask_rows
    }
    id_index = {(_masksql_question_id(row), _norm_text_for_match(row.get("orig_db_id"))): row for row in mask_rows}

    selected: List[Dict[str, Any]] = []
    used_masksql_ids = set()
    match_counter = {"strict": 0, "relaxed": 0, "id": 0, "unmatched": 0}

    for item in data:
        strict_key = _bird_match_key(item)
        relaxed_key = (strict_key[0], strict_key[1], strict_key[3])
        qid_key = (_safe_question_id(item.get("question_id")), _norm_text_for_match(item.get("db_id")))
        match_row = None
        method = None

        if strict_key in strict_index:
            match_row = strict_index[strict_key]
            method = "strict"
        elif match_mode in {"relaxed", "auto"} and relaxed_key in relaxed_index:
            match_row = relaxed_index[relaxed_key]
            method = "relaxed"
        elif match_mode in {"id", "auto"} and qid_key in id_index:
            match_row = id_index[qid_key]
            method = "id"

        if match_row is None:
            match_counter["unmatched"] += 1
            continue

        match_counter[method] += 1
        used_masksql_ids.add(match_row.get("idx"))
        new_item = copy.deepcopy(item)
        new_item["masksql_idx"] = match_row.get("idx")
        new_item["masksql_match_method"] = method
        new_item["masksql_orig_question"] = match_row.get("orig_question", "")
        new_item["masksql_orig_evidence"] = match_row.get("orig_evidence", "")
        new_item["masksql_orig_gold_sql"] = match_row.get("orig_gold_sql", "")
        new_item["orig_gold_schema_links"] = match_row.get("orig_gold_schema_links", {}) or {}
        new_item["orig_gold_value_links"] = match_row.get("orig_gold_value_links", {}) or {}
        new_item["annotated_links"] = _combine_gold_links(match_row)
        selected.append(new_item)

    report = {
        "masksql_test_path": masksql_test_path,
        "match_mode": match_mode,
        "input_rows": len(data),
        "masksql_rows": len(mask_rows),
        "selected_rows": len(selected),
        "matched_masksql_rows": len(used_masksql_ids),
        "match_counter": match_counter,
    }
    return selected, report


def _norm_surface(value: Any) -> str:
    text = _norm_text_for_match(value).strip("'\"` ").lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _surface_in_text(term: str, text: str) -> bool:
    term_norm = _norm_surface(term)
    text_norm = _norm_surface(text)
    if not term_norm:
        return False
    if re.fullmatch(r"[a-z0-9_]+", term_norm):
        return re.search(r"(?<![a-z0-9_])" + re.escape(term_norm) + r"(?![a-z0-9_])", text_norm) is not None
    return term_norm in text_norm


def _is_mask_placeholder(value: Any) -> bool:
    return re.fullmatch(r"(?:table|col|val)_\d+", _norm_surface(value)) is not None


def _collect_runtime_sensitive_surfaces(
    structured_db_schema: Dict[str, Any],
    entities: List[Dict[str, Any]],
    unmask_dict: Dict[str, Any],
) -> List[str]:
    """Collect sensitive surfaces available locally at inference time.

    This set contains all raw table/column identifiers plus entity/value strings
    found by the local extractor.  It deliberately does not use gold privacy
    annotations, which are unavailable at deployment time.
    """
    surfaces: List[str] = []
    for table_name, table_info in (structured_db_schema or {}).items():
        surfaces.append(str(table_name))
        columns = (table_info or {}).get("columns", []) if isinstance(table_info, dict) else []
        if isinstance(columns, dict):
            surfaces.extend(str(name) for name in columns)
        elif isinstance(columns, list):
            for column in columns:
                if isinstance(column, dict) and column.get("column_name") is not None:
                    surfaces.append(str(column["column_name"]))

    for entity in entities or []:
        if not isinstance(entity, dict):
            continue
        for key in ("entity", "table", "column", "cell_value"):
            value = entity.get(key)
            if value is not None and str(value).strip():
                surfaces.append(str(value))

    for mapping in (unmask_dict or {}).values():
        if isinstance(mapping, dict):
            surfaces.extend(str(value) for value in mapping.values())

    deduped = []
    seen = set()
    for surface in surfaces:
        normalized = _norm_surface(surface)
        if not normalized or _is_mask_placeholder(normalized) or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(surface)
    return deduped


def _term_covered_by_masked_terms(term: str, masked_terms: List[str]) -> bool:
    term_norm = _norm_surface(term)
    if not term_norm:
        return False
    for masked_term in masked_terms or []:
        mt = _norm_surface(masked_term)
        if not mt:
            continue
        if term_norm == mt or term_norm in mt or mt in term_norm:
            return True
    return False


ATTACK_PROMPT_AGENT_V1 = """
Your goal is to guess the original words in a masked Text-to-SQL input. Given
(1) NL Text: a masked natural-language instruction/question for extracting information from a database
(2) DB Schema: the masked database schema used by the remote SQL-generation model

guess the original word or phrase for each masked symbol that appears in the NL text.

Important: This prompt is adapted from MaskSQL's attack_v2 prompt to match this agent's mask format.

Input Format:
- DB Schema is the masked schema text produced by the privacy agent. It may be semi-structured rather than YAML.
- Table names, column names, and literal values are replaced consistently in both NL Text and DB Schema.
- Some column descriptions or value descriptions may also be masked.

Mask Format:
- table_1, table_2, ... are symbols used to mask table names.
- col_1, col_2, ... are symbols used to mask column names.
- val_1, val_2, ... are symbols used to mask literal values.
- The same symbol always refers to the same original object/value inside this case.

Output Rules:
- Output ONLY the unmasked NL text.
- The output should contain all of the unmasked tokens in the NL text; no portion of the text should be omitted.
- Do not output a JSON object, dictionary, bullet list, explanation, or comments.
- If you cannot confidently recover a mask, make your best guess while keeping the original sentence structure.
- For column names with spaces, wrap them in backticks, e.g. `car model`.

Example 1:
NL Text: What is the col_1 of table_1 who have a table_2 with col_4 higher than val_1? val_1 is a value of col_4
DB Schema:
table_1:
  col_1: text
  col_2: text
table_2:
  col_3: text
  col_4: number

OUTPUT:
What is the name of people who have a car with horse power higher than 1000? 1000 is a value of horse power

Now, unmask the following text considering the following DB schema.
Inputs:
NL Text: {question}
DB Schema:
{schema}
"""


def _masksql_exact_attack_recovered(term: str, attack_text: str) -> bool:
    """Match MaskSQL Results._process_row: term.lower() in attack.lower()."""
    if term is None or attack_text is None:
        return False
    term_text = str(term).strip().lower()
    attack = str(attack_text).lower()
    return bool(term_text) and term_text in attack


def compute_privacy_metrics_for_result(res: Dict[str, Any], attack_text: str = "") -> Dict[str, Any]:
    """
    MaskSQL-style privacy evaluation.

    MR follows MaskSQL's released code idea: annotated sensitive terms are counted
    as covered when they appear in the list of original terms actually masked by
    the system. RI follows MaskSQL's exact-match attack scoring when attack_text
    is supplied; otherwise it is left as None.
    """
    annotated_links = res.get("annotated_links") or {}
    annotated_terms = list(annotated_links.keys())
    masked_terms = res.get("masked_terms") or []
    masked_question = res.get("masked_question") or ""
    masked_schema = res.get("masked_schema") or ""
    masked_prompt = f"{masked_question}\n{masked_schema}"
    original_prompt = f"{res.get('question', '')} {res.get('evidence', '')} {res.get('final_question', '')}"

    covered_terms = [t for t in annotated_terms if _term_covered_by_masked_terms(t, masked_terms)]
    terms_present_in_input = [t for t in annotated_terms if _surface_in_text(t, original_prompt)]
    covered_present_terms = [t for t in terms_present_in_input if _term_covered_by_masked_terms(t, masked_terms)]
    direct_leak_terms = [t for t in annotated_terms if _surface_in_text(t, masked_prompt)]

    denominator = len(annotated_terms)
    present_denominator = len(terms_present_in_input)
    mr = (len(covered_terms) / denominator) if denominator else 1.0
    mr_present = (len(covered_present_terms) / present_denominator) if present_denominator else 1.0
    prompt_privacy_score = (1.0 - len(direct_leak_terms) / denominator) if denominator else 1.0

    # Extra privacy metrics used by the ablation runner:
    # - Privacy-P: among all terms this agent masked, how many match gold sensitive terms.
    # - Privacy-R: among all gold sensitive terms, how many were covered by masking.
    # - Privacy-F1: harmonic mean of Privacy-P and Privacy-R.
    # - LeakRate: fraction of gold sensitive terms that still directly appear in the masked prompt.
    gt_sensitive_count = denominator
    masked_cover_count = len(covered_terms)
    predicted_masked_count = len(masked_terms)
    direct_leak_count = len(direct_leak_terms)

    privacy_p = (
        masked_cover_count / predicted_masked_count
        if predicted_masked_count
        else (1.0 if gt_sensitive_count == 0 else 0.0)
    )
    privacy_r = (masked_cover_count / gt_sensitive_count) if gt_sensitive_count else 1.0
    privacy_f1 = (
        2 * privacy_p * privacy_r / (privacy_p + privacy_r)
        if (privacy_p + privacy_r) > 0
        else 0.0
    )
    leak_rate = (direct_leak_count / gt_sensitive_count) if gt_sensitive_count else 0.0

    ri = None
    recovered_terms = []
    if attack_text:
        # Follow MaskSQL's public implementation: a mask is considered
        # re-identified when the original masked term is a lowercase substring
        # of the attacker's generated unmasked NL text.
        recovered_terms = [t for t in masked_terms if _masksql_exact_attack_recovered(t, attack_text)]
        ri = 1.0 - (len(recovered_terms) / len(masked_terms)) if masked_terms else 1.0

    return {
        "masksql_idx": res.get("masksql_idx"),
        "case_id": res.get("case_id"),
        "db_id": res.get("db_id"),
        "match_method": res.get("masksql_match_method"),
        "gt_sensitive_count": denominator,
        "gt_sensitive_present_count": present_denominator,
        "masked_terms_count": len(masked_terms),
        "masked_cover_count": len(covered_terms),
        "masked_cover_present_count": len(covered_present_terms),
        "MR": mr,
        "MR_present": mr_present,
        "Privacy-P": privacy_p,
        "Privacy-R": privacy_r,
        "Privacy-F1": privacy_f1,
        "LeakRate": leak_rate,
        "prompt_direct_leak_count": len(direct_leak_terms),
        "prompt_direct_leak_terms": direct_leak_terms,
        "prompt_privacy_score": prompt_privacy_score,
        "RI": ri,
        "attack_recovered_count": len(recovered_terms),
        "attack_recovered_terms": recovered_terms,
        "annotated_terms": annotated_terms,
        "covered_terms": covered_terms,
    }


def _run_reidentification_attack(client: OpenAI, model_name: str, masked_question: str, masked_schema: str) -> Tuple[str, Dict[str, int]]:
    """Optional RI attack following MaskSQL attack_v2 semantics.

    MaskSQL asks the attacker to output the unmasked NL text, not a token list.
    This version keeps that assumption but adapts the prompt from [T1]/[C1]/[V1]
    masks to this agent's table_1/col_1/val_1 masks and schema text format.
    """
    if not str(masked_question or "").strip():
        return "", _empty_usage()
    prompt = ATTACK_PROMPT_AGENT_V1.format(
        question=str(masked_question or ""),
        schema=str(masked_schema or ""),
    )
    messages = [{"role": "user", "content": prompt}]
    response = client.chat.completions.create(model=model_name, messages=messages, temperature=0)
    return response.choices[0].message.content.strip(), _usage_to_dict(response)


def evaluate_privacy_results(
    results_list: List[Dict[str, Any]],
    output_path: str,
    attack_client: OpenAI = None,
    attack_model_name: str = "",
) -> Dict[str, Any]:
    per_case = []
    for res in results_list:
        attack_text = ""
        ri_usage = _empty_usage()
        # MaskSQL only scores RI over the actual masked_terms. If there are no
        # masked terms, skip the remote attack call and let RI remain None/1.0
        # according to compute_privacy_metrics_for_result.
        if attack_client is not None and attack_model_name and res.get("masked_terms"):
            try:
                attack_text, ri_usage = _run_reidentification_attack(
                    attack_client,
                    attack_model_name,
                    res.get("masked_question", ""),
                    res.get("masked_schema", ""),
                )
            except Exception as e:
                logging.warning(f"RI attack failed for case {res.get('case_id')}: {e}")
                attack_text = ""
        metrics = compute_privacy_metrics_for_result(res, attack_text=attack_text)
        metrics["ri_attack_token_usage"] = ri_usage
        if attack_text:
            metrics["attack_text"] = attack_text
        per_case.append(metrics)

    def _avg(key: str):
        vals = [row[key] for row in per_case if row.get(key) is not None]
        return mean(vals) if vals else None

    ri_token_usage_total = _empty_usage()
    for row in per_case:
        _add_usage(ri_token_usage_total, row.get("ri_attack_token_usage") or _empty_usage())

    aggregate = {
        "num_cases": len(per_case),
        "MR": _avg("MR"),
        "MR_present": _avg("MR_present"),
        "Privacy-P": _avg("Privacy-P"),
        "Privacy-R": _avg("Privacy-R"),
        "Privacy-F1": _avg("Privacy-F1"),
        "LeakRate": _avg("LeakRate"),
        "prompt_privacy_score": _avg("prompt_privacy_score"),
        "RI": _avg("RI"),
        "total_gt_sensitive_count": sum(row["gt_sensitive_count"] for row in per_case),
        "total_masked_cover_count": sum(row["masked_cover_count"] for row in per_case),
        "total_prompt_direct_leak_count": sum(row["prompt_direct_leak_count"] for row in per_case),
        "ri_attack_token_usage_total": ri_token_usage_total,
        "avg_ri_attack_total_tokens_per_case": (
            ri_token_usage_total["total_tokens"] / len(per_case) if per_case else 0.0
        ),
    }
    report = {"aggregate": aggregate, "per_case": per_case}
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return report
        
class PrivacyProtectionAgent:
    def __init__(
        self,
        base_url: str,
        model_name: str,
        api_key: str = "EMPTY",
        db_base_dir: str = "",
        log_dir: str = "case_logs",
        request_timeout: float = 120.0,
        enable_pretransmission_gate: bool = True,
        privacy_gate_reextract: bool = False,
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.client = OpenAI(api_key=api_key, base_url=base_url, timeout=request_timeout)
        self.model_name = model_name
        self.temperature = 0
        self.db_base_dir = db_base_dir
        self.log_dir = log_dir
        self.request_timeout = request_timeout
        self.enable_pretransmission_gate = enable_pretransmission_gate
        self.privacy_gate_reextract = privacy_gate_reextract

        self.structural_keys = {
            "columns", "column_name", "column_definition", "data_type", 
            "is_primary_key", "foreign_keys_info", "referenced_by", "examples", 
            "column_description", "value_description", "table_primary_keys", 
            "table_foreign_keys", "unique_constraints"
        }
        
        self.system_prompt = """
You are an NL2SQL privacy protection agent. You need to use the ReAct (Reasoning and Acting) paradigm to complete the privacy desensitization task for natural language queries. You must ensure that the desensitized natural language query still allows the model to write the correct SQL query.
Under this paradigm, you must alternate between Thought and Action, and after each action, proceed to the next step based on the received Observation, until reaching a final conclusion.

Sensitive Data Definition:
Any entity related to database tables, columns, or values present in the natural language query is treated as sensitive data.

[Processing Flow and Chain of Thought]
Please strictly follow these steps:
1. Evaluate Structure: Analyze the user's original Query to evaluate whether its privacy entities can be directly replaced. If a direct replacement destroys the syntactic structure (for example, replacing "How much does Alice earn per month?" directly into "How much dose [value_1] [column_1]" making it obvious that the relationship between [value_1] and [column_1] is lost), please call the `query_rewrite` tool to rewrite it.
2. Extract Entities: Using the finally decided Query (original or rewritten), call the `entity_extract` tool to extract all privacy entities.
3. Masking Operation: After obtaining the entity list, call the `apply_typed_masking` tool to mask the schema and the user's question respectively.
4. Evaluate Masking: Evaluate whether the masked query and schema are reasonable.
   - If the sentence structure is incomplete or differs significantly from the original question, making it unclear what the query is asking, it indicates that `query_rewrite` is needed to adjust the query sentence structure before masking again.
   - If there are unmasked entities, it means the entities extracted in the `entity_extract` step are incomplete. You need to directly supplement the missed entities you observed into `entities` and call `apply_typed_masking` again.
5. Generate SQL: Pass the masked query and schema to `sql_generate` to generate the desensitized SQL.
   - A local privacy gate runs before every `sql_generate` request. It rejects empty or stale masking outputs and any payload that still contains a locally detected raw table, column, or value. A rejected request is not sent to the cloud; use its feedback to extract the remaining entity and mask again.
6. Unmask SQL: After getting the generated desensitized SQL, call the `sql_unmask` tool, passing in the SQL and the reverse mapping dictionary (`unmask_dict`) to restore the real SQL.
7. Execute SQL Check: Once you have the real unmasked SQL, call the `execute_sql_check` tool to verify if it is syntactically correct and can be successfully executed against the database.
8. Autonomous Semantic Evaluation & Repair: After receiving the execution result (Observation) from `execute_sql_check`, you MUST independently and autonomously evaluate the SQL.
   - Review Execution Errors: If the tool returns a syntax or execution error, analyze why it failed.
   - Review Semantic Logic: Even if the execution is "Success", check if the SQL accurately answers the Original User Question.
   - Autonomously Repair: If there are execution errors OR semantic flaws, you must autonomously REPAIR it in your mind. Write down your error analysis and correction plan in your `Thought`. You may call `execute_sql_check` again with your repaired SQL to verify it.
   - Once verified and semantically sound, proceed to final output.

[Output Requirements]
Before deciding to call any tool, you must first output your thought process in your answer! Each response should follow this logic:
Thought: [Write down your thought process here. Analyze the current input, the previous Observation, what next step you need to take, and why.]
Action: [Execute your action by triggering the corresponding function call]

After the tool executes, the system will return the result to you as an Observation.

When you have completed all steps and obtained the final correct real SQL (after your autonomous evaluation, execution check, and potential repair):
Thought: [State that you have successfully verified the unmasked SQL execution and semantic correctness. If it needed repair, explain what was fixed. State that the task is complete.]
Directly output the final real SQL as the final response, without calling any tools again. You need to place the final SQL in the ```sql\n\n``` markdown format.
"""
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "query_rewrite",
                    "description": "Call this tool when the original query formulation is irregular. If called multiple times, a rewrite instruction (rewrite_instruction) can be passed to correct previous errors.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query_to_rewrite": {
                                "type": "string",
                                "description": "The original query sentence to be rewritten"
                            },
                            "rewrite_instruction": {
                                "type": "string",
                                "description": "(Optional) Additional rewrite requirements or instructions."
                            }
                        },
                        "required": ["query_to_rewrite"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "entity_extract",
                    "description": "Extract the Schema Linking entity information array.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query_to_extract": {"type": "string"}
                        },
                        "required": ["query_to_extract"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "apply_typed_masking",
                    "description": "Simultaneously perform co-frequency table, column, and value masking replacement on the Query and Structured Schema at the underlying level.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "entities": {
                                "type": "array", 
                                "items": {"type": "object"}
                            }
                        },
                        "required": ["query", "entities"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "sql_generate",
                    "description": "Generate the final SQL statement based on the masked query and masked Schema.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "masked_query": {
                                "type": "string",
                                "description": "The desensitized user question"
                            },
                            "masked_schema_str": {
                                "type": "string",
                                "description": "The desensitized database Schema string"
                            }
                        },
                        "required": ["masked_query", "masked_schema_str"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "sql_unmask",
                    "description": "Restore the masked SQL to the real SQL statement.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "masked_sql": {
                                "type": "string",
                                "description": "The desensitized SQL generated by sql_generate"
                            },
                            "unmask_dict": {
                                "type": "object",
                                "description": "The reverse mapping dictionary"
                            }
                        },
                        "required": ["masked_sql", "unmask_dict"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "execute_sql_check",
                    "description": "Check if the unmasked SQL can be executed successfully in the database. Returns 'Success' or an execution error message to help you repair the SQL.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "unmasked_sql": {
                                "type": "string",
                                "description": "The real SQL statement to check execution"
                            }
                        },
                        "required": ["unmasked_sql"]
                    }
                }
            }
        ]
        self.tool_specs = _tool_schema_specs(self.tools)


    def _tool_query_rewrite(self, db_schema: str, query_to_rewrite: str, rewrite_instruction: str = "", logger=None, token_usage=None) -> str:
        rewrite_client = OpenAI(
            api_key=os.getenv("QUERY_REWRITE_API_KEY", "EMPTY"),
            base_url=os.getenv("QUERY_REWRITE_BASE_URL", "http://8.130.67.139:8037/v1"),
            timeout=self.request_timeout,
        )
        model_name = os.getenv("QUERY_REWRITE_MODEL_NAME", str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-50'))
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_rewrite}"
        if rewrite_instruction:
            input_str += f"\n- Rewrite Instruction:\n{rewrite_instruction}"
        messages = get_query_rewrite_messages(input_str)
        response = rewrite_client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=self.temperature,
            max_tokens=get_env_max_tokens("QUERY_REWRITE_MAX_TOKENS", 512),
        )
        _record_response_usage(token_usage, "query_rewrite", response, include_in_sql_generation_total=True)
        ans = response.choices[0].message.content.strip()
        return ans

    def _tool_entity_extract(self, db_schema: str, query_to_extract: str, logger=None, token_usage=None) -> str:
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_extract}"
        messages = get_entity_extract_template(input_str)
        entity_extract_client = OpenAI(
            api_key=os.getenv("ENTITY_EXTRACT_API_KEY", "EMPTY"),
            base_url=os.getenv("ENTITY_EXTRACT_BASE_URL", "http://8.130.67.139:8036/v1"),
            timeout=self.request_timeout,
        )
        model_name = os.getenv("ENTITY_EXTRACT_MODEL_NAME", str(_RELEASE_ROOT / 'LlamaFactory/saves/entity_extract_v1/checkpoint-25'))
        response = entity_extract_client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=self.temperature,
            max_tokens=get_env_max_tokens("ENTITY_EXTRACT_MAX_TOKENS", 512),
        )
        _record_response_usage(token_usage, "entity_extract", response, include_in_sql_generation_total=True)
        response_text = response.choices[0].message.content.strip()
        match = re.search(r'\[\s*\{.*?\}\s*\]', response_text, re.DOTALL)
        return match.group(0) if match else "[]"
    
    def _tool_sql_generate(self, masked_query, masked_schema_str, logger=None, token_usage=None):
        sql_cfg = get_sql_generate_api_config()
        sql_generate_client = OpenAI(
            api_key=sql_cfg["api_key"],
            base_url=sql_cfg["base_url"],
            timeout=self.request_timeout,
        )
        model_name = sql_cfg["model_name"]

        # sql_generate_client = OpenAI(
        #     api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),  
        #     base_url="http://127.0.0.1:2333/v1"  
        # )
        # model_name = '/root/models/Qwen2.5-72B-Instruct'

        input_str = f"- Database Schema:\n{masked_schema_str}\n- User's Question:\n{masked_query}"
        messages = get_sql_generate_template(input_str)
        response = sql_generate_client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=self.temperature,
            max_tokens=get_env_max_tokens("SQL_GENERATE_MAX_TOKENS", 1024),
        )
        _record_response_usage(token_usage, "sql_generate", response, include_in_sql_generation_total=True)
        response_text = response.choices[0].message.content.strip()
        
        return response_text

    def _check_before_cloud_call(
        self,
        *,
        masked_query: str,
        masked_schema_str: str,
        latest_masked_query: str,
        latest_masked_schema: str,
        db_schema: str,
        structured_db_schema: Dict[str, Any],
        last_entities: List[Dict[str, Any]],
        unmask_dict: Dict[str, Any],
        logger=None,
        token_usage=None,
    ) -> Dict[str, Any]:
        """Apply the mandatory local rule before one cloud SQL call.

        The call is blocked when the payload is empty, differs from the latest
        output of the masking tool, or still contains a locally known raw
        table/column/value surface.  With re-extraction enabled, the local
        extractor inspects the candidate query once more and its findings are
        added to the scan.  Gold evaluation labels are never used here.
        """
        reasons: List[str] = []
        if not str(masked_query or "").strip() or not str(masked_schema_str or "").strip():
            reasons.append("empty masked query or schema")
        if str(latest_masked_query or "").strip() and str(masked_query or "").strip() != str(latest_masked_query).strip():
            reasons.append("query differs from latest masking-tool output")
        if str(latest_masked_schema or "").strip() and str(masked_schema_str or "").strip() != str(latest_masked_schema).strip():
            reasons.append("schema differs from latest masking-tool output")

        verifier_entities: List[Dict[str, Any]] = []
        verifier_error = ""
        if self.privacy_gate_reextract and str(masked_query or "").strip():
            try:
                verifier_raw = self._tool_entity_extract(
                    db_schema,
                    masked_query,
                    logger=logger,
                    token_usage=token_usage,
                )
                verifier_entities = _coerce_entity_list(verifier_raw)
            except Exception as exc:
                verifier_error = f"{type(exc).__name__}: {exc}"
                # Fail closed: a checker failure cannot authorize transmission.
                reasons.append("local residual-entity verifier failed")

        protected_surfaces = _collect_runtime_sensitive_surfaces(
            structured_db_schema,
            list(last_entities or []) + verifier_entities,
            unmask_dict,
        )
        payload = f"{masked_query}\n{masked_schema_str}"
        detected_surfaces = sorted(
            {surface for surface in protected_surfaces if _surface_in_text(surface, payload)},
            key=_norm_surface,
        )
        if detected_surfaces:
            reasons.append("raw sensitive surfaces remain in candidate payload")

        return {
            "allowed": not reasons,
            "blocked_reasons": reasons,
            "detected_surfaces": detected_surfaces,
            "verifier_entities": verifier_entities,
            "verifier_error": verifier_error,
        }

    def _tool_sql_unmask(self, masked_sql: str, unmask_dict: Any) -> str:
        if isinstance(unmask_dict, str):
            unmask_dict = json_repair.loads(unmask_dict)
        if not masked_sql or not isinstance(masked_sql, str) or not isinstance(unmask_dict, dict):
            return masked_sql
        mask_info_list = []
        for category in ["tables", "columns", "values"]:
            if category in unmask_dict:
                for mask, orig in unmask_dict[category].items():
                    mask_info_list.append({"mask": mask, "original": str(orig), "category": category})
        mask_info_list.sort(key=lambda x: len(x["mask"]), reverse=True)
        unmasked_text = masked_sql
        for info in mask_info_list:
            mask, original_value, category = info["mask"], info["original"], info["category"]
            pattern = re.compile(r'\b' + re.escape(mask) + r'\b', re.IGNORECASE)
            def replace_match(match):
                text = match.string
                start, end = match.start(), match.end()
                left_char = text[start - 1] if start > 0 else ""
                right_char = text[end] if end < len(text) else ""
                val = original_value
                if category == "columns" and "." in val:
                    tbl_part, col_part = val.split(".", 1)
                    if left_char == ".":
                        val = col_part
                    else:
                        res = f"`{tbl_part}`.`{col_part}`"
                        if left_char not in ("`", "'", '"') and right_char not in ("`", "'", '"'): return res
                        return f"{tbl_part}.{col_part}"
                if category in ["tables", "columns"]:
                    if left_char not in ("`", "'", '"') and right_char not in ("`", "'", '"'): return f"`{val}`"
                return val
            unmasked_text = pattern.sub(replace_match, unmasked_text)
        return unmasked_text

    def _tool_execute_sql_check(self, unmasked_sql: str, db_path: str) -> str:
        if not unmasked_sql or not db_path:
            return "Error: Missing SQL or Database Path."
        
        try:
            conn = sqlite3.connect(db_path)
            check_sql = f"SELECT * FROM ({unmasked_sql.strip().rstrip(';')}) LIMIT 1;"
            safe_execute_sql(conn, check_sql, timeout_sec=15.0)
            return "Success"
        except TimeoutError:
            return "SQL Execution Error: Execution timed out (exceeded 15 seconds). The query might be too complex or have an infinite loop (e.g., Cartesian product)."
        except Exception as e:
            return f"SQL Execution Error: {str(e)}"
        finally:
            if 'conn' in locals():
                conn.close()

    def _infer_textual_tool_args(
        self,
        func_name: str,
        args: Dict[str, Any],
        *,
        original_question: str,
        evidence: str,
        final_question: str,
        masked_question: str,
        masked_schema: str,
        masked_sql: str,
        unmask_dict: Dict[str, Any],
        last_entities: List[Dict[str, Any]],
        last_unmasked_sql: str,
    ) -> Dict[str, Any]:
        """Fill recoverable arguments for text-only ReAct actions.

        Cold-start checkpoints often emit natural-language ReAct actions rather
        than OpenAI/XML function-call payloads. These fallbacks let the harness
        execute the intended tool while still recording the turn as a textual
        protocol action in the audit.
        """
        args = _coerce_tool_arguments(args)
        if not isinstance(args, dict):
            args = {}
        args.pop("_raw_arguments", None)

        question_with_evidence = f"{original_question} {evidence}".strip()
        active_question = (final_question or question_with_evidence or original_question).strip()
        if func_name == "query_rewrite":
            args.setdefault("query_to_rewrite", active_question)
        elif func_name == "entity_extract":
            args.setdefault("query_to_extract", active_question)
        elif func_name == "apply_typed_masking":
            args.setdefault("query", active_question)
            args.setdefault("entities", last_entities or [])
        elif func_name == "sql_generate":
            args.setdefault("masked_query", masked_question or active_question)
            args.setdefault("masked_schema_str", masked_schema)
        elif func_name == "sql_unmask":
            args.setdefault("masked_sql", masked_sql)
            args.setdefault("unmask_dict", unmask_dict or {})
        elif func_name == "execute_sql_check":
            args.setdefault("unmasked_sql", last_unmasked_sql or masked_sql)
        return args

    def _tool_apply_typed_masking(self, query, entities, structured_db_schema):
        table_map, col_map, val_map = {}, {}, {}
        t_idx, c_idx, v_idx = 1, 1, 1
        
        for table_name, table_info in structured_db_schema.items():
            if not table_name or not str(table_name).strip(): continue 
            if table_name not in table_map:
                table_map[table_name] = f"table_{t_idx}"
                t_idx += 1
            cols = table_info["columns"] if "columns" in table_info else [{"column_name": k, **v} for k, v in table_info.items()]
            for col_info in cols:
                col_name = col_info.get("column_name", "")
                if not col_name or not str(col_name).strip(): continue 
                if (table_name, col_name) not in col_map:
                    col_map[(table_name, col_name)] = f"col_{c_idx}"
                    c_idx += 1

        for ent in entities:
            if ent.get("entity_type") == "cell_value" and ent.get("cell_value") is not None:
                val_str = str(ent["cell_value"])
                if not val_str.strip(): continue
                if val_str not in val_map:
                    val_map[val_str] = f"val_{v_idx}"
                    v_idx += 1

        def replace_in_string(text):
            if not isinstance(text, str) or not text: return text
            replace_dict = {orig: mask for orig, mask in table_map.items()}
            replace_dict.update({orig_c: mask for (orig_t, orig_c), mask in col_map.items()})
            replace_dict.update({orig: mask for orig, mask in val_map.items()})
            sorted_keys = sorted(replace_dict.keys(), key=lambda k: len(str(k)), reverse=True)
            res = text
            for k in sorted_keys:
                k_str = str(k)
                if not k_str: continue
                if re.match(r'^\w+$', k_str):
                    res = re.sub(r'\b' + re.escape(k_str) + r'\b', replace_dict[k], res, flags=re.IGNORECASE)
                else:
                    res = res.replace(k_str, replace_dict[k])
            return res

        masked_query = query
        masked_terms = []
        mask_mapping = {}
        sorted_entities = sorted(entities, key=lambda x: len(str(x.get("entity", ""))), reverse=True)
        for ent in sorted_entities:
            ent_str = str(ent.get("entity", ""))
            if not ent_str.strip(): continue
            mask = None
            if ent.get("entity_type") == "table": mask = table_map.get(ent.get("table"))
            elif ent.get("entity_type") == "column": mask = col_map.get((ent.get("table"), ent.get("column")))
            elif ent.get("entity_type") == "cell_value": mask = val_map.get(str(ent.get("cell_value")))
            if mask:
                pattern = re.compile(r'\b' + re.escape(ent_str) + r'\b', re.IGNORECASE) if re.match(r'^\w+$', ent_str) else re.compile(re.escape(ent_str), re.IGNORECASE)
                before = masked_query
                masked_query = pattern.sub(mask, masked_query)
                if before != masked_query:
                    masked_terms.append(ent_str)
                    mask_mapping[ent_str] = mask

        # Fallback replacement can also mask schema/table/value terms not explicitly
        # listed as extracted entities. Record those replacements for MR evaluation.
        before_fallback = masked_query
        masked_query = replace_in_string(masked_query)
        fallback_replace_dict = {orig: mask for orig, mask in table_map.items()}
        fallback_replace_dict.update({orig_c: mask for (orig_t, orig_c), mask in col_map.items()})
        fallback_replace_dict.update({orig: mask for orig, mask in val_map.items()})
        for orig, mask in fallback_replace_dict.items():
            if _surface_in_text(mask, masked_query) and _surface_in_text(orig, before_fallback):
                if str(orig) not in mask_mapping:
                    masked_terms.append(str(orig))
                    mask_mapping[str(orig)] = mask

        masked_schema = {}
        for table_name, table_info in structured_db_schema.items():
            if not table_name or not str(table_name).strip(): continue
            m_table_name = table_map.get(table_name, table_name)
            m_table_info = {}
            if "columns" in table_info:
                m_table_info["columns"] = []
                for col_info in table_info["columns"]:
                    m_col_info = {}
                    for attr_key, attr_val in col_info.items():
                        if attr_key == "examples": continue 
                        m_col_info[attr_key] = replace_in_string(attr_val) if isinstance(attr_val, str) else attr_val
                    orig_col_name = col_info.get("column_name", "")
                    m_col_info["column_name"] = col_map.get((table_name, orig_col_name), orig_col_name)
                    m_table_info["columns"].append(m_col_info)
                for k, v in table_info.items():
                    if k not in ["columns", "examples"]: 
                        m_table_info[k] = [replace_in_string(item) if isinstance(item, str) else item for item in v] if isinstance(v, list) else replace_in_string(v) if isinstance(v, str) else v
            else:
                for col_name, col_attrs in table_info.items():
                    m_col_name = col_map.get((table_name, col_name), col_name)
                    m_col_attrs = {}
                    for attr_key, attr_val in col_attrs.items():
                        if attr_key == "examples": continue
                        m_col_attrs[attr_key] = replace_in_string(attr_val) if isinstance(attr_val, str) else attr_val
                    m_table_info[m_col_name] = m_col_attrs
            masked_schema[m_table_name] = m_table_info

        unmask_dict = {
            "tables": {mask: orig for orig, mask in table_map.items()},
            "columns": {mask: f"{orig_t}.{orig_c}" for (orig_t, orig_c), mask in col_map.items()},
            "values": {mask: orig for orig, mask in val_map.items()}
        }
        masked_schema_str = generate_schema_string_from_masked(masked_schema)
        dedup_masked_terms = list(dict.fromkeys(masked_terms))
        return {
            "masked_query": masked_query,
            "masked_schema_str": masked_schema_str,
            "unmask_dict": unmask_dict,
            "masked_terms": dedup_masked_terms,
            "mask_mapping": mask_mapping,
        }

    def process(self, item: Dict[str, Any], case_id: str = "default") -> Dict[str, str]:
        
        os.makedirs(self.log_dir, exist_ok=True)
        log_file_path = os.path.join(self.log_dir, f"case_{case_id}.txt")
        
        if os.path.exists(log_file_path):
            open(log_file_path, 'w', encoding='utf-8').close()

        def log_to_file(text: str):
            with open(log_file_path, "a", encoding="utf-8") as f:
                f.write(text + "\n")

        original_question = item['question']
        db_schema = item.get('db_schema', '') 
        evidence = item.get("evidence", '')
        db_path = os.path.join(self.db_base_dir, item['db_id'], item['db_id'] + '.sqlite')
        structured_db_schema = item.get('structured_db_schema', {}) 
        
        user_prompt = f"- Database Schema:\n{db_schema}\n\n- User Question:\n{original_question} {evidence}\n\nTask started. Please provide your analysis (Thought) and call the tools (Action)."

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        masked_schema = ''
        masked_sql = ''
        unmask_dict = {}
        masked_question = ''
        masked_terms = []
        mask_mapping = {}
        last_entities: List[Dict[str, Any]] = []
        last_unmasked_sql = ''
        last_textual_tool_name = ''
        consecutive_textual_tool_count = 0
        outbound_trace: List[Dict[str, Any]] = []
        final_question = original_question  # 默认最终问题为原始问题
        token_usage = _new_token_usage_container()
        tool_use_audit = _new_tool_audit()
        
        for iteration in range(20):
            log_to_file(f"\n{'='*20} [Round {iteration + 1}] {'='*20}")
            
            # ==========================================================
            # 1. API 请求环节：强制关闭原生解析，绕过服务端 400 报错
            # ==========================================================
            max_try = 5
            response = None
            while max_try > 0:
                try:
                    response = self.client.chat.completions.create(
                        model=self.model_name, 
                        messages=messages, 
                        tools=self.tools, 
                        temperature=self.temperature,
                        max_tokens=get_agent_max_tokens(),
                    )
                    break
                except Exception as e:
                    logging.error(f"API 调用失败 (剩余重试 {max_try-1}): {str(e)}")
                    max_try -= 1
                    # time.sleep(2)
                    continue

            if not response:
                log_to_file("API 调用多次重试后彻底失败")
                break

            # Count local agent/controller LLM calls as part of the SQL-generation pipeline.
            # If a backend does not return usage, num_missing_usage is incremented.
            _record_response_usage(token_usage, "agent", response, include_in_sql_generation_total=True)

            message = response.choices[0].message
            original_content = message.content or ""
            
            # ==========================================================
            # 2. 客户端解析环节：只提取第一个工具，强行切断复读
            # ==========================================================
            parsed_tool_calls = []
            
            # 使用 re.search 只匹配文本中出现的第一个 <tool_call>
            match = re.search(r'<tool_call>\s*(.*?)\s*</tool_call>', original_content, re.DOTALL)
            parse_failed = False
            
            if match:
                try:
                    tool_name, tool_args = _normalize_tool_call_payload(match.group(1))
                    if not tool_name:
                        raise ValueError("tool_call payload has no tool name")
                    parsed_tool_calls.append({
                        "name": tool_name,
                        "arguments": tool_args,
                        "id": f"call_patch_{iteration}_0" # 只有一个工具，固定编号 0
                    })
                except Exception as e:
                    parse_failed = True
                    _record_tool_parse_error(tool_use_audit, iteration, match.group(1), e)
                    log_to_file(f"⚠️ 工具解析失败: {e}\n原文: {match.group(1)}")
            elif not parsed_tool_calls:
                textual_tool_name, textual_tool_args = _extract_textual_action(original_content)
                if textual_tool_name:
                    if textual_tool_name == last_textual_tool_name:
                        consecutive_textual_tool_count += 1
                    else:
                        last_textual_tool_name = textual_tool_name
                        consecutive_textual_tool_count = 1
                    if consecutive_textual_tool_count <= 2:
                        textual_tool_args = self._infer_textual_tool_args(
                            textual_tool_name,
                            textual_tool_args,
                            original_question=original_question,
                            evidence=evidence,
                            final_question=final_question,
                            masked_question=masked_question,
                            masked_schema=masked_schema,
                            masked_sql=masked_sql,
                            unmask_dict=unmask_dict,
                            last_entities=last_entities,
                            last_unmasked_sql=last_unmasked_sql,
                        )
                        parsed_tool_calls.append({
                            "name": textual_tool_name,
                            "arguments": textual_tool_args,
                            "id": f"call_text_{iteration}_0",
                            "textual_action_only": True,
                        })
                    else:
                        log_to_file(f"⚠️ Repeated textual action {textual_tool_name}; stopping to avoid a loop.")

            # 打印 Thought 日志（把后面的所有 XML 标签都清洗掉）
            thought_text = re.sub(r'<tool_call>.*?</tool_call>', '', original_content, flags=re.DOTALL).strip()
            log_to_file(f"🧠 [Thought]:\n{thought_text if thought_text else '(Direct tool output)'}\n")

            # ==========================================================
            # 3. 构造合法的 Assistant 历史消息，骗过服务端的历史校验
            # ==========================================================
            assistant_msg = {"role": "assistant", "content": original_content}
            
            if parsed_tool_calls:
                # 🌟 核心机制：如果我们自己解析到了工具，我们需要手动伪造一个原生的 tool_calls 列表
                # 否则下一轮发给 API 时，服务端会报错 "tool_calls cannot be empty"
                assistant_msg["tool_calls"] = [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": json.dumps(tc["arguments"], ensure_ascii=False) # 变回干净的无换行字符串
                        }
                    }
                    for tc in parsed_tool_calls
                ]
            
            # 追加合法的助手消息
            messages.append(assistant_msg) 

            # ==========================================================
            # 4. 执行工具逻辑，并追加 Tool 历史消息
            # ==========================================================
            if parsed_tool_calls:
                for tc_data in parsed_tool_calls:
                    func_name = tc_data["name"]
                    args = _coerce_tool_arguments(tc_data.get("arguments", {}))
                    textual_action_only = bool(tc_data.get("textual_action_only"))
                    if textual_action_only:
                        args = self._infer_textual_tool_args(
                            func_name,
                            args,
                            original_question=original_question,
                            evidence=evidence,
                            final_question=final_question,
                            masked_question=masked_question,
                            masked_schema=masked_schema,
                            masked_sql=masked_sql,
                            unmask_dict=unmask_dict,
                            last_entities=last_entities,
                            last_unmasked_sql=last_unmasked_sql,
                        )
                    tool_call_id = tc_data["id"]
                    log_args_str = json.dumps(args, ensure_ascii=False, indent=2)
                    
                    log_to_file(f"🛠️ [Action - Tool Call]: {func_name}")
                    log_to_file(f"   [Action - Arguments]: {log_args_str}\n")
                    audit_turn = _audit_tool_call(tool_use_audit, iteration, func_name, args, self.tool_specs)
                    if textual_action_only:
                        audit_turn["textual_action_only"] = True
                        audit_turn["has_tool_call"] = False
                        audit_turn["content_preview"] = original_content[:500]
                        tool_use_audit["num_parse_errors"] += 1
                    
                    # --- 执行你的工具逻辑 ---
                    try:
                        if func_name == "query_rewrite": 
                            result = self._tool_query_rewrite(db_schema, args.get('query_to_rewrite', ''), args.get('rewrite_instruction', ''), logger=log_to_file, token_usage=token_usage)
                            if isinstance(result, str) and result.strip():
                                final_question = result.strip()
                        elif func_name == "entity_extract": 
                            result = self._tool_entity_extract(db_schema, args.get('query_to_extract', ''), logger=log_to_file, token_usage=token_usage)
                            last_entities = _coerce_entity_list(result)
                        elif func_name == "apply_typed_masking": 
                            entities_arg = args.get('entities', [])
                            if isinstance(entities_arg, str):
                                entities_arg = _coerce_json_object(entities_arg, default=[])
                            if not isinstance(entities_arg, list):
                                entities_arg = []
                            result = self._tool_apply_typed_masking(args.get('query', ''), entities_arg, structured_db_schema)
                            if isinstance(result, dict):
                                masked_schema = result.get("masked_schema_str", masked_schema)
                                masked_question = result.get("masked_query", masked_question)
                                unmask_dict = result.get("unmask_dict", unmask_dict)
                                masked_terms = result.get("masked_terms", masked_terms)
                                mask_mapping = result.get("mask_mapping", mask_mapping)
                        elif func_name == "sql_generate":
                            candidate_query = str(args.get('masked_query', ''))
                            candidate_schema = str(args.get('masked_schema_str', ''))
                            gate_result = {
                                "allowed": True,
                                "blocked_reasons": [],
                                "detected_surfaces": [],
                                "verifier_entities": [],
                                "verifier_error": "",
                            }
                            if self.enable_pretransmission_gate:
                                gate_result = self._check_before_cloud_call(
                                    masked_query=candidate_query,
                                    masked_schema_str=candidate_schema,
                                    latest_masked_query=masked_question,
                                    latest_masked_schema=masked_schema,
                                    db_schema=db_schema,
                                    structured_db_schema=structured_db_schema,
                                    last_entities=last_entities,
                                    unmask_dict=unmask_dict,
                                    logger=log_to_file,
                                    token_usage=token_usage,
                                )
                            trace_entry = {
                                "attempt_index": len(outbound_trace) + 1,
                                "iteration": iteration + 1,
                                "allowed": bool(gate_result["allowed"]),
                                "actual_cloud_call": False,
                                "masked_query": candidate_query,
                                "masked_schema_str": candidate_schema,
                                "blocked_reasons": gate_result["blocked_reasons"],
                                "detected_surfaces": gate_result["detected_surfaces"],
                                "verifier_error": gate_result["verifier_error"],
                            }
                            outbound_trace.append(trace_entry)
                            if not gate_result["allowed"]:
                                audit_turn["privacy_gate_blocked"] = True
                                audit_turn["privacy_gate_detected_surfaces"] = gate_result["detected_surfaces"]
                                result = (
                                    "Privacy Gate Blocked: no cloud request was sent. "
                                    "Reasons: " + "; ".join(gate_result["blocked_reasons"]) + ". "
                                    "Residual surfaces: " + json.dumps(gate_result["detected_surfaces"], ensure_ascii=False) + ". "
                                    "Re-run entity extraction and masking before retrying sql_generate."
                                )
                            else:
                                masked_schema = candidate_schema
                                masked_question = candidate_query
                                result = self._tool_sql_generate(
                                    candidate_query,
                                    candidate_schema,
                                    logger=log_to_file,
                                    token_usage=token_usage,
                                )
                                trace_entry["actual_cloud_call"] = True
                                masked_sql = _extract_sql_text(result)
                        elif func_name == "sql_unmask":
                            masked_sql = args.get('masked_sql', '')
                            unmask_dict = args.get('unmask_dict', {})
                            result = self._tool_sql_unmask(args.get('masked_sql', ''), unmask_dict)
                            last_unmasked_sql = _extract_sql_text(result)
                        elif func_name == "execute_sql_check":
                            result = self._tool_execute_sql_check(args.get('unmasked_sql', ''), db_path)
                        else: 
                            result = f"Error: Unknown tool {func_name}"
                    except Exception as tool_error:
                        result = f"Tool Execution Error in {func_name}: {type(tool_error).__name__}: {tool_error}"
                        log_to_file(traceback.format_exc())
                    if isinstance(result, str) and (
                        result.startswith("Error:")
                        or result.startswith("Tool Execution Error")
                        or result.startswith("SQL Execution Error")
                    ):
                        tool_use_audit["num_tool_execution_errors"] += 1
                        audit_turn["tool_execution_error"] = True
                        audit_turn["tool_execution_error_message"] = result[:500]
                    
                    # 格式化结果
                    tool_content = json.dumps(result, ensure_ascii=False) if isinstance(result, (dict, list)) else str(result)
                    log_to_file(f"👀 [Observation - Result]:\n{tool_content}\n")
                    
                    # 追加原生的 tool 角色消息
                    messages.append({"role": "tool", "tool_call_id": tool_call_id, "name": func_name, "content": tool_content})

            else:
                # 如果代码走到这里，说明没有匹配到 <tool_call>，代表任务结束或直接作答
                if not match:
                    if not _record_textual_action_audit(tool_use_audit, iteration, original_content, self.tool_specs):
                        _record_no_tool_turn(tool_use_audit, iteration, original_content)
                    tool_use_audit["completed_with_final_answer"] = True
                elif parse_failed:
                    tool_use_audit["completed_with_final_answer"] = False
                log_to_file(f"\n{'='*30} [CASE END] {'='*30}")
                log_to_file(f"[DB SCHEMA]:\n{db_schema}\n")
                log_to_file(f"[USER QUESTION]:\n{original_question}\n")
                log_to_file(f"[EVIDENCE]:\n{evidence}\n")
                log_to_file(f"✅ [GOLD SQL]:\n{item.get('gold_sql', '')}\n")
                log_to_file(f"=======================================================================")

                log_to_file(f"[MASKED DB SCHEMA]:\n{masked_schema}\n")
                log_to_file(f"[MASKED QUESTION]:\n{masked_question}\n")
                log_to_file(f"[MASKED SQL]:\n{masked_sql}\n")
                log_to_file(f"[MASKED DICT]:\n{unmask_dict}\n")
                log_to_file(f"[MASKED TERMS]:\n{json.dumps(masked_terms, ensure_ascii=False)}\n")

                log_to_file(f"✅ [Final Answer]:\n{thought_text}\n")
                
                return {
                    "result_text": thought_text,
                    "final_question": final_question,
                    "masked_question": masked_question,
                    "masked_schema": masked_schema,
                    "masked_sql": masked_sql,
                    "unmask_dict": unmask_dict,
                    "masked_terms": masked_terms,
                    "mask_mapping": mask_mapping,
                    "outbound_trace": outbound_trace,
                    "token_usage": token_usage,
                    "tool_use_audit": tool_use_audit,
                }
        log_to_file(f"\n{'='*30} [CASE TIMEOUT] {'='*30}")
        log_to_file("Agent exceeded maximum iterations.")
        tool_use_audit["timed_out"] = True
        return {
            "result_text": '{"error": "Agent timeout"}',
            "final_question": final_question,
            "masked_question": masked_question,
            "masked_schema": masked_schema,
            "masked_sql": masked_sql,
            "unmask_dict": unmask_dict,
            "masked_terms": masked_terms,
            "mask_mapping": mask_mapping,
            "outbound_trace": outbound_trace,
            "token_usage": token_usage,
            "tool_use_audit": tool_use_audit,
        }


# ==========================================
# New Module: Independently extracted single-thread processing function
# ==========================================
def process_single_item(idx, item, agent: PrivacyProtectionAgent, total_num):
    case_id = str(item.get("question_id", f"idx_{idx}"))
    db_id = item.get('db_id', 'unknown_db')
    gold_sql = item.get('gold_sql', '')

    # BIRD execution evaluator must use the original BIRD dev index, not the
    # subset-local enumerate index. MaskSQL rows store it as "bird_{idx}".
    eval_original_idx = item.get("question_id", None)
    if eval_original_idx is None and item.get("masksql_idx"):
        masksql_idx_str = str(item.get("masksql_idx"))
        if masksql_idx_str.startswith("bird_"):
            eval_original_idx = masksql_idx_str.replace("bird_", "", 1)
    if eval_original_idx is None:
        eval_original_idx = idx
    eval_original_idx = str(eval_original_idx)
    
    logging.info(f"⏳ Starting to process case [{idx+1}/{total_num}] - {case_id}, eval_original_idx={eval_original_idx}")
    try:
        process_res = agent.process(item, case_id=case_id)
        result_text = process_res.get("result_text", "")
        final_question = process_res.get("final_question", item.get("question", ""))
        masked_question = process_res.get("masked_question", "")
        pred_sql = find_result(result_text)
        process_res["tool_use_audit"] = _finalize_tool_audit(process_res.get("tool_use_audit") or _new_tool_audit(), pred_sql)
        logging.info(f"✅ Finished case [{idx+1}/{total_num}] - {case_id}")
    except Exception as e:
        logging.error(f"❌ Case failed [{idx+1}/{total_num}] - {case_id}: {type(e).__name__}: {e}")
        logging.error(traceback.format_exc())
        process_res = {
            "result_text": json.dumps({
                "error": "case failed",
                "exception_type": type(e).__name__,
                "message": str(e),
            }, ensure_ascii=False),
            "final_question": item.get("question", ""),
            "masked_question": "",
            "masked_schema": "",
            "masked_sql": "",
            "unmask_dict": {},
            "masked_terms": [],
            "mask_mapping": {},
            "outbound_trace": [],
            "token_usage": _new_token_usage_container(),
            "tool_use_audit": _finalize_tool_audit(_new_tool_audit(), ""),
        }
        result_text = process_res["result_text"]
        final_question = process_res["final_question"]
        masked_question = process_res["masked_question"]
        pred_sql = ""

    return {
        "idx": idx,
        "eval_original_idx": eval_original_idx,
        "case_id": case_id,
        "db_id": db_id,
        "question": item.get('question', ''),
        "evidence": item.get('evidence', ''),
        "final_question": final_question,
        "masked_question": masked_question,
        "masked_schema": process_res.get("masked_schema", ""),
        "masked_sql": process_res.get("masked_sql", ""),
        "unmask_dict": process_res.get("unmask_dict", {}),
        "masked_terms": process_res.get("masked_terms", []),
        "mask_mapping": process_res.get("mask_mapping", {}),
        "outbound_trace": process_res.get("outbound_trace", []),
        "token_usage": process_res.get("token_usage", _new_token_usage_container()),
        "tool_use_audit": process_res.get("tool_use_audit", _finalize_tool_audit(_new_tool_audit(), pred_sql)),
        "gold_sql": gold_sql,
        "result_text": result_text,
        "pred_sql": pred_sql,
        "masksql_idx": item.get("masksql_idx"),
        "masksql_match_method": item.get("masksql_match_method"),
        "orig_gold_schema_links": item.get("orig_gold_schema_links", {}),
        "orig_gold_value_links": item.get("orig_gold_value_links", {}),
        "annotated_links": item.get("annotated_links", {}),
    }

# ==========================================
# Core logic of the evaluation module
# ==========================================
def eval_exec(predicted_sql_path, db_root_path, data_mode, ground_truth_path, num_cpus, mode_gt, mode_predict, diff_json_path, meta_time_out, output_dir, inference_data_name):
    if 'spider' in inference_data_name:
        predicted_sql_path = predicted_sql_path.replace('json', 'sql')
        # Spider path replacement logic. Prefer caller-provided local paths, and
        # only fall back to historical hard-coded paths when they exist.
        default_spider_db = '/root/data/nl2sql/spider/database'
        if (not db_root_path or not os.path.isdir(db_root_path)) and os.path.isdir(default_spider_db):
            db_root_path = default_spider_db
        local_spider_root = str(_RELEASE_ROOT / 'privacy-agent/CHESS/spider')
        if 'syn' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_syn_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        elif 'real' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_real_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        elif 'test' in inference_data_name:
            if not ground_truth_path or not os.path.exists(ground_truth_path):
                ground_truth_path = os.path.join(local_spider_root, 'test_gold.sql')
            table_path = os.path.join(local_spider_root, 'test_tables.json')
        elif 'dev' in inference_data_name:
            if not ground_truth_path or not os.path.exists(ground_truth_path):
                ground_truth_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        else:
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        
        eval_script = Path(__file__).resolve().parent / 'spider_eval' / 'evaluation.py'
        eval_cmd = f'''python {shlex.quote(str(eval_script))} \
        --gold {shlex.quote(ground_truth_path)} \
        --pred {shlex.quote(predicted_sql_path)} \
        --etype all \
        --db {shlex.quote(db_root_path)} \
        --table {shlex.quote(table_path)} > {shlex.quote(os.path.join(output_dir, "eval.log"))} 2>&1
        ''' 
    else:
        # BIRD logic
        eval_script = Path(__file__).resolve().parent / 'bird_eval' / 'evaluation.py'
        eval_cmd = f'''python {shlex.quote(str(eval_script))} \
    --db_root_path {shlex.quote(db_root_path)} \
    --predicted_sql_path {shlex.quote(predicted_sql_path)} \
    --data_mode {shlex.quote(data_mode)} \
    --ground_truth_path {shlex.quote(ground_truth_path)} \
    --num_cpus {num_cpus} \
    --mode_gt {shlex.quote(mode_gt)} \
    --mode_predict {shlex.quote(mode_predict)} \
    --diff_json_path {shlex.quote(diff_json_path)} \
    --meta_time_out {meta_time_out} \
    --output_dir {shlex.quote(output_dir)}
    ''' 
    logging.info(f"🚀 Starting to execute the evaluation script...\n{eval_cmd}")
    subprocess.run(eval_cmd, shell=True, check=True)

def run_evaluation(
    agent: PrivacyProtectionAgent,
    data_path: str,
    db_root_path: str,
    output_dir: str,
    inference_data_name: str,
    diff_json_path: str,
    data_mode: str,
    ground_truth_path: str,
    max_workers: int = 10,
    masksql_test_path: str = None,
    masksql_match_mode: str = "auto",
    run_ri_attack: bool = False,
    ri_attack_base_url: str = None,
    ri_attack_model_name: str = None,
    ri_attack_api_key: str = "EMPTY",
    case_log_dir: str = None,
    bird_eval_num_cpus: int = 4,
    eval_sample_offset: int = 0,
    max_eval_samples: int = 0,
):
    """
    Use Agent to perform multi-threaded concurrent inference on the dataset, extract SQL, and run tests
    """
    logging.info(f"Loading dataset: {data_path}")
    data = read_data(data_path)
    masksql_match_report = None
    if masksql_test_path:
        data, masksql_match_report = filter_and_attach_masksql_annotations(
            data=data,
            masksql_test_path=masksql_test_path,
            match_mode=masksql_match_mode,
        )
        logging.info(f"MaskSQL annotated subset report: {json.dumps(masksql_match_report, ensure_ascii=False)}")
    if eval_sample_offset < 0:
        raise ValueError("eval_sample_offset must be non-negative")
    if eval_sample_offset:
        data = data[eval_sample_offset:]
        logging.info(f"Skipped the first eval_sample_offset={eval_sample_offset} selected rows")
    if max_eval_samples and max_eval_samples > 0 and len(data) > max_eval_samples:
        data = data[:max_eval_samples]
        logging.info(f"Limited evaluation rows to max_eval_samples={max_eval_samples}")
    total_data = len(data)
    if total_data == 0:
        raise ValueError("No data rows selected for evaluation. Check data_path, masksql_test_path, and masksql_match_mode.")
    
    # Build output path for predictions and eval
    reasoning_file_dir = os.path.join(output_dir, inference_data_name, "agent_eval")
    os.makedirs(reasoning_file_dir, exist_ok=True)
    agent.db_base_dir = db_root_path
    agent.log_dir = case_log_dir or os.path.join(reasoning_file_dir, "case_logs")
    os.makedirs(agent.log_dir, exist_ok=True)
    
    pred_sql_path = os.path.join(reasoning_file_dir, 'predict.sql')
    json_result_path = os.path.join(reasoning_file_dir, 'predict.json')
    all_res_path = os.path.join(reasoning_file_dir, 'all_res.json')
    privacy_eval_path = os.path.join(reasoning_file_dir, 'privacy_eval.json')
    token_usage_path = os.path.join(reasoning_file_dir, 'token_usage.json')
    tool_use_audit_path = os.path.join(reasoning_file_dir, 'tool_use_audit.json')
    tool_use_audit_csv_path = os.path.join(reasoning_file_dir, 'tool_use_audit.csv')
    tool_use_audit_md_path = os.path.join(reasoning_file_dir, 'tool_use_audit.md')
    masksql_match_report_path = os.path.join(reasoning_file_dir, 'masksql_match_report.json')

    results_list = []
    logging.info(f"⚡ Starting multi-threaded concurrent processing, max workers: {max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_item, idx, item, agent, total_data): idx 
            for idx, item in enumerate(data)
        }
        for future in as_completed(futures):
            res = future.result()
            results_list.append(res)
           

    results_list.sort(key=lambda x: x["idx"])

    all_res = []
    bird_eval_dic = {}
    
    with open(pred_sql_path, 'w', encoding='utf-8') as f_sql:
        for res in results_list:
            f_sql.write(res["pred_sql"] + '\n')
            # Use original BIRD dev index as predict.json key. This is required
            # when only a MaskSQL-annotated subset is evaluated.
            eval_key = str(res.get("eval_original_idx", res["idx"]))
            bird_eval_dic[eval_key] = res["pred_sql"] + '\t----- bird -----\t' + res["db_id"]
            
            # 追加新增的 final_question 和 masked_question
            all_res_entry = {
                'eval_original_idx': eval_key,
                'question_id': res.get("case_id"),
                'case_id': res.get("case_id"),
                'db_id': res["db_id"],
                'input': res["question"],
                'evidence': res.get("evidence", ""),
                'final_question': res["final_question"],
                'masked_question': res["masked_question"],
                'masked_schema': res.get("masked_schema", ""),
                'masked_sql': res.get("masked_sql", ""),
                'unmask_dict': res.get("unmask_dict", {}),
                'masked_terms': res.get("masked_terms", []),
                'mask_mapping': res.get("mask_mapping", {}),
                'outbound_trace': res.get("outbound_trace", []),
                'token_usage': res.get("token_usage", {}),
                'tool_use_audit': res.get("tool_use_audit", {}),
                'total_tokens': (res.get("token_usage", {}).get("sql_generation_total", {}) or {}).get("total_tokens", 0),
                'prompt_tokens': (res.get("token_usage", {}).get("sql_generation_total", {}) or {}).get("prompt_tokens", 0),
                'completion_tokens': (res.get("token_usage", {}).get("sql_generation_total", {}) or {}).get("completion_tokens", 0),
                'output': res["result_text"],
                'pred_sql': res["pred_sql"],
                'gold_sql': res["gold_sql"],
                'masksql_idx': res.get("masksql_idx"),
                'masksql_match_method': res.get("masksql_match_method"),
                'orig_gold_schema_links': res.get("orig_gold_schema_links", {}),
                'orig_gold_value_links': res.get("orig_gold_value_links", {}),
                'annotated_links': res.get("annotated_links", {}),
            }
            if masksql_test_path:
                privacy_metrics = compute_privacy_metrics_for_result(res)
                all_res_entry.update({
                    "MR": privacy_metrics.get("MR"),
                    "MR_present": privacy_metrics.get("MR_present"),
                    "Privacy-P": privacy_metrics.get("Privacy-P"),
                    "Privacy-R": privacy_metrics.get("Privacy-R"),
                    "Privacy-F1": privacy_metrics.get("Privacy-F1"),
                    "LeakRate": privacy_metrics.get("LeakRate"),
                    "prompt_privacy_score": privacy_metrics.get("prompt_privacy_score"),
                    "prompt_direct_leak_count": privacy_metrics.get("prompt_direct_leak_count"),
                    "gt_sensitive_count": privacy_metrics.get("gt_sensitive_count"),
                    "masked_cover_count": privacy_metrics.get("masked_cover_count"),
                    "masked_terms_count": privacy_metrics.get("masked_terms_count"),
                })
            all_res.append(all_res_entry)

    # Save JSON
    with open(json_result_path, 'w', encoding='utf-8') as f_json:
        json.dump(bird_eval_dic, f_json, indent=4)
        
    with open(all_res_path, 'w', encoding='utf-8') as f_all:
        json.dump(all_res, f_all, ensure_ascii=False, indent=4)

    token_usage_report = _aggregate_token_usage(results_list)
    with open(token_usage_path, 'w', encoding='utf-8') as f_token:
        json.dump(token_usage_report, f_token, ensure_ascii=False, indent=2)
    logging.info(
        "Token usage aggregate: "
        + json.dumps({
            "avg_total_tokens_per_sql_generation": token_usage_report["average_total_tokens_per_sql_generation"],
            "total_tokens_for_sql_generation": token_usage_report["total_tokens_for_sql_generation"],
        }, ensure_ascii=False)
    )

    tool_audit_report = aggregate_tool_use_audit(results_list)
    write_tool_use_audit_reports(
        tool_audit_report,
        output_json=tool_use_audit_path,
        output_csv=tool_use_audit_csv_path,
        output_md=tool_use_audit_md_path,
    )
    logging.info(f"Tool-use audit aggregate: {json.dumps(tool_audit_report, ensure_ascii=False)}")

    if masksql_match_report is not None:
        with open(masksql_match_report_path, 'w', encoding='utf-8') as f_match:
            json.dump(masksql_match_report, f_match, ensure_ascii=False, indent=2)

    if masksql_test_path:
        attack_client = None
        if run_ri_attack:
            if not ri_attack_base_url or not ri_attack_model_name:
                raise ValueError("run_ri_attack=True requires ri_attack_base_url and ri_attack_model_name.")
            attack_client = OpenAI(api_key=ri_attack_api_key, base_url=ri_attack_base_url, timeout=agent.request_timeout)
        privacy_report = evaluate_privacy_results(
            results_list=results_list,
            output_path=privacy_eval_path,
            attack_client=attack_client,
            attack_model_name=ri_attack_model_name or "",
        )
        logging.info(f"Privacy evaluation aggregate: {json.dumps(privacy_report['aggregate'], ensure_ascii=False)}")

    # Execute external script evaluation
    eval_kwargs = {
        'predicted_sql_path': json_result_path, 
        'db_root_path': db_root_path, 
        'output_dir': reasoning_file_dir, 
        'diff_json_path': diff_json_path, 
        'data_mode': data_mode, 
        'num_cpus': bird_eval_num_cpus, 
        'mode_gt': 'gt', 
        'mode_predict': 'gpt', 
        'ground_truth_path': ground_truth_path, 
        'meta_time_out': 30,
        'inference_data_name': inference_data_name
    }
    eval_exec(**eval_kwargs)
    logging.info(f"✅ Evaluation completed! Results are stored in: {reasoning_file_dir}")



# ==========================================
# Multi-checkpoint deployment and evaluation
# ==========================================
@dataclass
class CheckpointJobResult:
    checkpoint: str
    checkpoint_name: str
    gpu: str
    port: int
    output_dir: str
    success: bool
    error: str = ""
    elapsed_sec: float = 0.0


def _checkpoint_sort_key(path: str):
    name = os.path.basename(os.path.normpath(path))
    nums = re.findall(r"\d+", name)
    return (int(nums[-1]) if nums else -1, name)


def discover_checkpoints(checkpoint_root: str, checkpoint_pattern: str = "checkpoint-*") -> List[str]:
    paths = sorted(
        [p for p in glob.glob(os.path.join(checkpoint_root, checkpoint_pattern)) if os.path.isdir(p)],
        key=_checkpoint_sort_key,
    )
    if not paths:
        raise FileNotFoundError(f"No checkpoints found under {checkpoint_root!r} with pattern {checkpoint_pattern!r}")
    return paths


def make_server_command(
    backend: str,
    model_path: str,
    host: str,
    port: int,
    gpu: str,
    template: str = "",
    chat_template: str = "qwen",
    extra_args: str = "",
) -> str:
    """
    Build the model-server command. The command is executed with CUDA_VISIBLE_DEVICES
    already set to the assigned GPU, but {gpu} is also available for custom templates.
    """
    values = {
        "model_path": model_path,
        "host": host,
        "port": port,
        "gpu": gpu,
        "chat_template": chat_template,
        "extra_args": extra_args,
    }
    if template:
        return template.format(**values)

    if backend == "llamafactory":
        # Works for many LLaMAFactory installations. If your local version uses a YAML
        # config instead, pass --server_cmd_template explicitly.
        return (
            "llamafactory-cli api "
            "--model_name_or_path {model_path} "
            "--template {chat_template} "
            "--infer_backend vllm "
            "--host {host} --port {port} "
            "{extra_args}"
        ).format(**values).strip()

    if backend == "vllm":
        return (
            "vllm serve {model_path} "
            "--served-model-name {model_path} "
            "--host {host} --port {port} "
            "--trust-remote-code "
            "{extra_args}"
        ).format(**values).strip()

    if backend == "lmdeploy":
        # Match the user's existing single-checkpoint deployment command:
        # lmdeploy serve api_server --log-level=INFO --server-port 2337 --tp=1 /path/to/checkpoint
        # CUDA_VISIBLE_DEVICES is set outside this command, so --tp=1 means each
        # checkpoint server uses exactly one assigned GPU.
        return (
            "lmdeploy serve api_server "
            "--log-level=INFO "
            "--server-port {port} "
            "--tp=1 "
            "{model_path} "
            "{extra_args}"
        ).format(**values).strip()

    raise ValueError(
        f"Unsupported server backend: {backend}. "
        "Use lmdeploy, llamafactory, vllm, or provide --server_cmd_template."
    )


def wait_for_openai_server(base_url: str, api_key: str, timeout_sec: int, poll_interval: float = 5.0) -> None:
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=10.0)
    start = time.time()
    last_err = ""
    while time.time() - start < timeout_sec:
        try:
            client.models.list()
            return
        except Exception as e:
            last_err = str(e)
            time.sleep(poll_interval)
    raise TimeoutError(f"Server health check failed for {base_url} after {timeout_sec}s. Last error: {last_err}")


def get_served_model_id(base_url: str, api_key: str, fallback_model_name: str, timeout: float = 30.0) -> str:
    """
    Query OpenAI-compatible /v1/models and return the model id exposed by the
    server. This is important for LMDeploy: chat.completions.create(model=...)
    should usually use the served model id rather than an arbitrary local path.
    """
    try:
        client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)
        models = client.models.list()
        if getattr(models, "data", None):
            model_id = getattr(models.data[0], "id", None)
            if model_id:
                return str(model_id)
    except Exception as e:
        logging.warning(f"Failed to get served model id from {base_url}/models: {e}")
    logging.warning(f"Using fallback model name: {fallback_model_name}")
    return fallback_model_name


def terminate_process_group(proc: subprocess.Popen, grace_sec: int = 20) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=grace_sec)
    except Exception:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            pass


def get_ri_attack_api_config(
    args: argparse.Namespace,
    local_agent_base_url: str,
    local_agent_model_name: str,
    local_agent_api_key: str,
) -> Tuple[str, str, str]:
    """Return RI-attack API config.

    By default, RI attack reuses the SQL-generation API model, because the
    attack is conceptually an external model attempting to recover masked terms.
    Use --no_ri_attack_same_as_sql_generate if you explicitly want the RI attack
    to use the local checkpoint agent server or another manually configured API.
    """
    if getattr(args, "ri_attack_same_as_sql_generate", True):
        sql_cfg = get_sql_generate_api_config()
        return sql_cfg["base_url"], sql_cfg["model_name"], sql_cfg["api_key"]

    ri_base_url = args.ri_attack_base_url or local_agent_base_url
    ri_model_name = args.ri_attack_model_name or local_agent_model_name
    ri_api_key = args.ri_attack_api_key or local_agent_api_key
    return ri_base_url, ri_model_name, ri_api_key


def evaluate_one_checkpoint(
    checkpoint_path: str,
    gpu: str,
    port: int,
    args: argparse.Namespace,
) -> CheckpointJobResult:
    ckpt_name = os.path.basename(os.path.normpath(checkpoint_path))
    ckpt_output_dir = os.path.join(args.output_dir, ckpt_name)
    server_log_dir = os.path.join(ckpt_output_dir, "server_logs")
    os.makedirs(server_log_dir, exist_ok=True)
    server_log_path = os.path.join(server_log_dir, f"server_gpu{gpu}_port{port}.log")

    start = time.time()
    server_proc = None
    try:
        base_url = f"http://{args.host}:{port}/v1"
        api_key = args.agent_api_key
        server_cmd = make_server_command(
            backend=args.server_backend,
            model_path=checkpoint_path,
            host=args.host,
            port=port,
            gpu=gpu,
            template=args.server_cmd_template,
            chat_template=args.chat_template,
            extra_args=args.server_extra_args,
        )

        if not args.no_auto_deploy:
            env = os.environ.copy()
            env["CUDA_VISIBLE_DEVICES"] = str(gpu)
            # Prevent tokenizer parallelism warnings from flooding logs.
            env.setdefault("TOKENIZERS_PARALLELISM", "false")
            logging.info(f"[{ckpt_name}] launching server on GPU {gpu}, port {port}: {server_cmd}")
            log_f = open(server_log_path, "w", encoding="utf-8")
            server_proc = subprocess.Popen(
                server_cmd,
                shell=True,
                stdout=log_f,
                stderr=subprocess.STDOUT,
                env=env,
                preexec_fn=os.setsid,
            )
            wait_for_openai_server(base_url, api_key, args.server_startup_timeout)
            logging.info(f"[{ckpt_name}] server is ready: {base_url}")
        else:
            logging.info(f"[{ckpt_name}] auto deploy disabled; using existing server: {base_url}")
            wait_for_openai_server(base_url, api_key, args.server_startup_timeout)

        served_model_name = get_served_model_id(
            base_url=base_url,
            api_key=api_key,
            fallback_model_name=checkpoint_path,
            timeout=min(args.request_timeout, 30.0),
        )
        logging.info(f"[{ckpt_name}] served model id: {served_model_name}")

        agent = PrivacyProtectionAgent(
            base_url=base_url,
            model_name=served_model_name,
            api_key=api_key,
            db_base_dir=args.db_base_dir,
            log_dir=os.path.join(ckpt_output_dir, "case_logs"),
            request_timeout=args.request_timeout,
            enable_pretransmission_gate=getattr(args, "enable_pretransmission_gate", True),
            privacy_gate_reextract=getattr(args, "privacy_gate_reextract", False),
        )

        ri_attack_base_url, ri_attack_model_name, ri_attack_api_key = get_ri_attack_api_config(
            args=args,
            local_agent_base_url=base_url,
            local_agent_model_name=served_model_name,
            local_agent_api_key=api_key,
        )
        if args.run_ri_attack:
            logging.info(
                f"[{ckpt_name}] RI attack API: base_url={ri_attack_base_url}, "
                f"model={ri_attack_model_name}, same_as_sql_generate={args.ri_attack_same_as_sql_generate}"
            )

        for inference_data_name in args.inference_data_names:
            data_path = os.path.join(args.eval_data_base_dir, f"{inference_data_name}.jsonl")
            logging.info(f"[{ckpt_name}] evaluating dataset: {inference_data_name}")
            run_evaluation(
                agent=agent,
                data_path=data_path,
                db_root_path=args.db_base_dir,
                output_dir=ckpt_output_dir,
                inference_data_name=inference_data_name,
                diff_json_path=args.diff_json_path,
                data_mode=args.data_mode,
                ground_truth_path=args.ground_truth_path,
                max_workers=args.per_checkpoint_workers,
                masksql_test_path=args.masksql_test_path,
                masksql_match_mode=args.masksql_match_mode,
                run_ri_attack=args.run_ri_attack,
                ri_attack_base_url=ri_attack_base_url,
                ri_attack_model_name=ri_attack_model_name,
                ri_attack_api_key=ri_attack_api_key,
                case_log_dir=os.path.join(ckpt_output_dir, inference_data_name, "agent_eval", "case_logs"),
                bird_eval_num_cpus=args.bird_eval_num_cpus,
                eval_sample_offset=args.eval_sample_offset,
                max_eval_samples=args.max_eval_samples,
            )

        return CheckpointJobResult(
            checkpoint=checkpoint_path,
            checkpoint_name=ckpt_name,
            gpu=str(gpu),
            port=port,
            output_dir=ckpt_output_dir,
            success=True,
            elapsed_sec=time.time() - start,
        )
    except Exception as e:
        logging.error(f"[{ckpt_name}] failed: {type(e).__name__}: {e}")
        logging.error(traceback.format_exc())
        return CheckpointJobResult(
            checkpoint=checkpoint_path,
            checkpoint_name=ckpt_name,
            gpu=str(gpu),
            port=port,
            output_dir=ckpt_output_dir,
            success=False,
            error=f"{type(e).__name__}: {e}",
            elapsed_sec=time.time() - start,
        )
    finally:
        if server_proc is not None and not args.keep_servers:
            logging.info(f"[{ckpt_name}] terminating server on GPU {gpu}, port {port}")
            terminate_process_group(server_proc)


def run_multi_checkpoint_evaluation(args: argparse.Namespace) -> List[CheckpointJobResult]:
    checkpoints = args.checkpoints or discover_checkpoints(args.checkpoint_root, args.checkpoint_pattern)
    if args.limit_checkpoints > 0:
        checkpoints = checkpoints[:args.limit_checkpoints]

    gpus = [g.strip() for g in args.gpus.split(",") if g.strip()]
    if not gpus:
        raise ValueError("No GPU ids provided. Example: --gpus 0,1,2,3,4,5,6,7")

    max_parallel = min(args.max_parallel, len(gpus), len(checkpoints))
    slots = Queue()
    for i, gpu in enumerate(gpus[:max_parallel]):
        slots.put((gpu, args.base_port + i))

    os.makedirs(args.output_dir, exist_ok=True)
    summary_path = os.path.join(args.output_dir, "multi_checkpoint_summary.json")

    results: List[CheckpointJobResult] = []
    logging.info(f"Found {len(checkpoints)} checkpoint(s). Running up to {max_parallel} in parallel on GPUs {gpus[:max_parallel]}.")

    def _job_wrapper(ckpt: str):
        gpu, port = slots.get()
        try:
            return evaluate_one_checkpoint(ckpt, gpu, port, args)
        finally:
            slots.put((gpu, port))

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(_job_wrapper, ckpt): ckpt for ckpt in checkpoints}
        for future in as_completed(futures):
            res = future.result()
            results.append(res)
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump([asdict(x) for x in sorted(results, key=lambda r: _checkpoint_sort_key(r.checkpoint))], f, ensure_ascii=False, indent=2)
            status = "OK" if res.success else "FAILED"
            logging.info(f"[{res.checkpoint_name}] {status}; elapsed={res.elapsed_sec:.1f}s; output={res.output_dir}")

    results.sort(key=lambda r: _checkpoint_sort_key(r.checkpoint))
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump([asdict(x) for x in results], f, ensure_ascii=False, indent=2)
    logging.info(f"All checkpoint jobs finished. Summary: {summary_path}")
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parallel multi-checkpoint evaluator for the privacy NL2SQL agent.")

    # Checkpoints and GPU scheduling
    parser.add_argument("--checkpoint_root", default=str(_RELEASE_ROOT / 'privacy-agent/verl/output/models/qwen/Qwen3-4B-Instruct/qwen3-4b_function_rm-gsm8k-sgl-multi-w-tool-verify-n16'))
    parser.add_argument("--checkpoint_pattern", default="checkpoint-*")
    parser.add_argument("--checkpoints", nargs="*", default=None, help="Optional explicit checkpoint directories. Overrides checkpoint_root/pattern.")
    parser.add_argument("--limit_checkpoints", type=int, default=0, help="Debug option; 0 means no limit.")
    parser.add_argument("--gpus", default="0,1,2,3,4,5,6,7")
    parser.add_argument("--max_parallel", type=int, default=8)
    parser.add_argument("--base_port", type=int, default=2337)
    parser.add_argument("--host", default="127.0.0.1")

    # Model server deployment
    parser.add_argument(
        "--server_backend",
        choices=["lmdeploy", "llamafactory", "vllm"],
        default="lmdeploy",
        help="Backend used when --server_cmd_template is not provided. Default is lmdeploy.",
    )
    parser.add_argument(
        "--server_cmd_template",
        default="",
        help=(
            "Custom command. Placeholders: {model_path}, {host}, {port}, "
            "{gpu}, {chat_template}, {extra_args}. For LMDeploy, the built-in default is: "
            "lmdeploy serve api_server --log-level=INFO --server-port {port} --tp=1 {model_path}"
        ),
    )
    parser.add_argument("--server_extra_args", default="")
    parser.add_argument("--chat_template", default="qwen")
    parser.add_argument("--server_startup_timeout", type=int, default=900)
    parser.add_argument("--keep_servers", action="store_true", help="Do not kill model servers after evaluation.")
    parser.add_argument("--no_auto_deploy", action="store_true", help="Do not launch servers; assume ports are already serving checkpoints.")
    parser.add_argument("--agent_api_key", default=os.getenv("AGENT_API_KEY", "EMPTY"))
    parser.add_argument("--request_timeout", type=float, default=120.0)

    # Data and evaluation paths
    parser.add_argument("--eval_data_base_dir", default=str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data'))
    parser.add_argument("--output_dir", default=str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/ineference/output_multi_ckpt'))
    parser.add_argument("--inference_data_names", nargs="+", default=["bird_dev_sc"])
    parser.add_argument("--masksql_test_path", default=None, help="Default: {eval_data_base_dir}/masksql_test.json")
    parser.add_argument("--masksql_match_mode", default="auto", choices=["strict", "relaxed", "id", "auto"])
    parser.add_argument("--eval_sample_offset", type=int, default=0, help="Skip this many rows after optional MaskSQL filtering, before applying max_eval_samples.")
    parser.add_argument("--max_eval_samples", type=int, default=0, help="Limit selected evaluation rows after optional MaskSQL filtering; 0 means no limit.")
    parser.add_argument(
        "--enable_pretransmission_gate",
        dest="enable_pretransmission_gate",
        action="store_true",
        default=True,
        help=(
            "Before every sql_generate call, block empty/stale payloads and payloads "
            "that still contain locally known raw table, column, or value surfaces."
        ),
    )
    parser.add_argument(
        "--disable_pretransmission_gate",
        dest="enable_pretransmission_gate",
        action="store_false",
        help="Reproduce the submitted inference path without the new mandatory gate.",
    )
    parser.add_argument(
        "--privacy_gate_reextract",
        action="store_true",
        help=(
            "Run the local entity extractor once more on each candidate outbound query. "
            "Only applies with --enable_pretransmission_gate; checker failures block the call."
        ),
    )
    parser.add_argument("--run_ri_attack", action="store_true")
    parser.add_argument(
        "--ri_attack_same_as_sql_generate",
        dest="ri_attack_same_as_sql_generate",
        action="store_true",
        default=True,
        help="Use SQL_GENERATE_* API config for RI attack. This is the default.",
    )
    parser.add_argument(
        "--no_ri_attack_same_as_sql_generate",
        dest="ri_attack_same_as_sql_generate",
        action="store_false",
        help="Do not reuse SQL_GENERATE_* for RI attack; use --ri_attack_* or local checkpoint server fallback.",
    )
    parser.add_argument("--ri_attack_base_url", default="", help="Only used when --no_ri_attack_same_as_sql_generate is set.")
    parser.add_argument("--ri_attack_model_name", default="", help="Only used when --no_ri_attack_same_as_sql_generate is set.")
    parser.add_argument("--ri_attack_api_key", default=os.getenv("RI_ATTACK_API_KEY", ""), help="Only used when --no_ri_attack_same_as_sql_generate is set.")

    # BIRD / Spider external evaluator settings
    parser.add_argument("--db_base_dir", default=os.getenv('BIRD_DEV_DB_BASE_DIR', str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/dev/dev_databases')))
    parser.add_argument("--diff_json_path", default=str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/dev/dev.json'))
    parser.add_argument("--data_mode", default="dev")
    parser.add_argument("--ground_truth_path", default=str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/dev/dev.sql'))
    parser.add_argument("--bird_eval_num_cpus", type=int, default=4)

    # Per-checkpoint in-dataset concurrency. Do not leave this too high when 8
    # model servers run simultaneously; otherwise the local servers and auxiliary
    # rewrite/extract/generate services may be overloaded.
    parser.add_argument("--per_checkpoint_workers", type=int, default=8)

    args = parser.parse_args()
    if args.masksql_test_path is None:
        args.masksql_test_path = os.path.join(args.eval_data_base_dir, "masksql_test.json")
    return args


if __name__ == "__main__":
    cli_args = parse_args()
    run_multi_checkpoint_evaluation(cli_args)
