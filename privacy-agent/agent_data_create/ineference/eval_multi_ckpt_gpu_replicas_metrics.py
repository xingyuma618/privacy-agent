
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))

import os
import json
import logging
from openai import OpenAI
from typing import Dict, List, Any, Tuple, Optional, Sequence, Union
import re
import subprocess
import time
import traceback
import sqlite3
import argparse
import glob
import shlex
import signal
import math
from dataclasses import dataclass, asdict, field
from pathlib import Path
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed 
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

# Shared SQL-generation API defaults.
SQL_GENERATE_DEFAULT_BASE_URL = "https://api.deepseek.com/v1"
SQL_GENERATE_DEFAULT_MODEL_NAME = "deepseek-v4-flash"
SQL_GENERATE_DEFAULT_API_KEY = "EMPTY"


def get_sql_generate_api_config() -> Dict[str, str]:
    return {
        "api_key": os.getenv("SQL_GENERATE_API_KEY", SQL_GENERATE_DEFAULT_API_KEY),
        "base_url": os.getenv("SQL_GENERATE_BASE_URL", SQL_GENERATE_DEFAULT_BASE_URL),
        "model_name": os.getenv("SQL_GENERATE_MODEL_NAME", SQL_GENERATE_DEFAULT_MODEL_NAME),
    }

# ==========================================
# Token usage utilities
# ==========================================
# Token usage reflects the model-call cost needed to produce one SQL prediction.
# We include all inference LLM calls used by the agent pipeline before the final
# SQL prediction:
#   - agent: local checkpoint/controller ReAct calls
#   - query_rewrite: local/trusted rewrite model tool call
#   - entity_extract: local/trusted entity extraction model tool call
#   - sql_generate: remote SQL-generation model call
# Pure Python tools such as apply_typed_masking/sql_unmask/execute_sql_check do
# not consume LM tokens and are not counted.
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
    """Aggregate per-case token usage into average SQL-generation totals."""
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
            "Average tokens per SQL generation. In this evaluator, "
            "sql_generation_total includes all model calls used by the agent "
            "pipeline to produce the final SQL: agent, query_rewrite, "
            "entity_extract, and sql_generate. Pure local Python tools such as "
            "apply_typed_masking, sql_unmask, and execute_sql_check do not "
            "consume LM tokens and are not counted."
        ),
        "average_total_tokens_per_sql_generation": by_component["sql_generation_total"]["avg_total_tokens_per_case"],
        "average_prompt_tokens_per_sql_generation": by_component["sql_generation_total"]["avg_prompt_tokens_per_case"],
        "average_completion_tokens_per_sql_generation": by_component["sql_generation_total"]["avg_completion_tokens_per_case"],
        "total_tokens_for_sql_generation": by_component["sql_generation_total"]["total_tokens"],
        "total_prompt_tokens_for_sql_generation": by_component["sql_generation_total"]["prompt_tokens"],
        "total_completion_tokens_for_sql_generation": by_component["sql_generation_total"]["completion_tokens"],
        "token_usage_scope": "agent_pipeline_model_calls_excluding_evaluation",
        "included_in_main_metric": ["agent", "query_rewrite", "entity_extract", "sql_generate"],
        "excluded_from_main_metric": ["apply_typed_masking", "sql_unmask", "execute_sql_check"],
        "by_component": by_component,
    }


# ==========================================
# Latency utilities
# ==========================================
# Latency is recorded separately from token usage because some OpenAI-compatible
# serving backends return usage but do not expose per-request timing. We measure
# wall-clock time in the evaluator for:
#   - case_latency_sec: end-to-end latency of one sample, including all agent
#     rounds, tool calls, SQL check, and local Python overhead.
#   - llm_total / by_component: latency of each LLM/API call.
#   - tool_total / by_tool: latency of each tool invocation, including local
#     tools and tool functions that themselves call LLM services.


def _empty_latency_stats() -> Dict[str, float]:
    return {
        "num_calls": 0,
        "total_sec": 0.0,
        "avg_sec_per_call": 0.0,
        "max_sec_per_call": 0.0,
    }


def _new_latency_container() -> Dict[str, Any]:
    return {
        "case_latency_sec": 0.0,
        "llm_total": _empty_latency_stats(),
        "tool_total": _empty_latency_stats(),
        "by_component": {component: _empty_latency_stats() for component in TOKEN_USAGE_COMPONENTS},
        "by_tool": {},
    }


def _add_latency_stat(dst: Dict[str, Any], elapsed_sec: float) -> None:
    elapsed_sec = float(elapsed_sec or 0.0)
    dst["num_calls"] = int(dst.get("num_calls", 0)) + 1
    dst["total_sec"] = float(dst.get("total_sec", 0.0)) + elapsed_sec
    dst["max_sec_per_call"] = max(float(dst.get("max_sec_per_call", 0.0)), elapsed_sec)
    dst["avg_sec_per_call"] = dst["total_sec"] / max(int(dst["num_calls"]), 1)


def _record_llm_latency(latency_stats: Optional[Dict[str, Any]], component: str, elapsed_sec: float) -> None:
    if latency_stats is None:
        return
    if component not in latency_stats.setdefault("by_component", {}):
        latency_stats["by_component"][component] = _empty_latency_stats()
    _add_latency_stat(latency_stats["by_component"][component], elapsed_sec)
    _add_latency_stat(latency_stats.setdefault("llm_total", _empty_latency_stats()), elapsed_sec)


def _record_tool_latency(latency_stats: Optional[Dict[str, Any]], tool_name: str, elapsed_sec: float) -> None:
    if latency_stats is None:
        return
    by_tool = latency_stats.setdefault("by_tool", {})
    if tool_name not in by_tool:
        by_tool[tool_name] = _empty_latency_stats()
    _add_latency_stat(by_tool[tool_name], elapsed_sec)
    _add_latency_stat(latency_stats.setdefault("tool_total", _empty_latency_stats()), elapsed_sec)


def _merge_latency_stats(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    """Merge aggregated latency call stats; case latency is handled separately."""
    for key in ["llm_total", "tool_total"]:
        src_stats = src.get(key) or {}
        if src_stats:
            dst_stats = dst.setdefault(key, _empty_latency_stats())
            dst_stats["num_calls"] = int(dst_stats.get("num_calls", 0)) + int(src_stats.get("num_calls", 0))
            dst_stats["total_sec"] = float(dst_stats.get("total_sec", 0.0)) + float(src_stats.get("total_sec", 0.0))
            dst_stats["max_sec_per_call"] = max(float(dst_stats.get("max_sec_per_call", 0.0)), float(src_stats.get("max_sec_per_call", 0.0)))
            dst_stats["avg_sec_per_call"] = dst_stats["total_sec"] / max(int(dst_stats["num_calls"]), 1)

    for section in ["by_component", "by_tool"]:
        src_section = src.get(section) or {}
        dst_section = dst.setdefault(section, {})
        for name, src_stats in src_section.items():
            dst_stats = dst_section.setdefault(name, _empty_latency_stats())
            dst_stats["num_calls"] = int(dst_stats.get("num_calls", 0)) + int(src_stats.get("num_calls", 0))
            dst_stats["total_sec"] = float(dst_stats.get("total_sec", 0.0)) + float(src_stats.get("total_sec", 0.0))
            dst_stats["max_sec_per_call"] = max(float(dst_stats.get("max_sec_per_call", 0.0)), float(src_stats.get("max_sec_per_call", 0.0)))
            dst_stats["avg_sec_per_call"] = dst_stats["total_sec"] / max(int(dst_stats["num_calls"]), 1)


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(float(v) for v in values)
    if len(values) == 1:
        return values[0]
    rank = (len(values) - 1) * p
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return values[int(rank)]
    return values[lo] * (hi - rank) + values[hi] * (rank - lo)


def _aggregate_latency_stats(
    results_list: List[Dict[str, Any]],
    inference_wall_sec: float = 0.0,
    max_workers: int = 0,
    num_agent_replicas: int = 1,
) -> Dict[str, Any]:
    num_cases = len(results_list)
    aggregate = _new_latency_container()
    case_latencies = []

    for res in results_list:
        latency = res.get("latency_stats") or {}
        case_latency = float(latency.get("case_latency_sec", 0.0) or 0.0)
        if case_latency > 0:
            case_latencies.append(case_latency)
        _merge_latency_stats(aggregate, latency)

    total_case_latency = sum(case_latencies)
    aggregate["case_latency_sec"] = {
        "num_cases": num_cases,
        "total_sec": total_case_latency,
        "avg_sec_per_case": total_case_latency / max(len(case_latencies), 1),
        "min_sec_per_case": min(case_latencies) if case_latencies else 0.0,
        "max_sec_per_case": max(case_latencies) if case_latencies else 0.0,
        "p50_sec_per_case": _percentile(case_latencies, 0.50),
        "p95_sec_per_case": _percentile(case_latencies, 0.95),
    }
    aggregate["inference_wall_sec"] = float(inference_wall_sec or 0.0)
    aggregate["throughput_cases_per_sec"] = num_cases / inference_wall_sec if inference_wall_sec > 0 else 0.0
    aggregate["max_workers"] = int(max_workers or 0)
    aggregate["num_agent_replicas"] = int(num_agent_replicas or 1)
    aggregate["latency_scope"] = "end_to_end_case_latency_and_evaluator_measured_llm_tool_latency"
    return aggregate

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

# ==========================================
# Generic text normalization helpers used by masking
# ==========================================
def _norm_text_for_match(value: Any) -> str:
    """Normalize text fields for robust string matching."""
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").strip()
    return re.sub(r"\s+", " ", text)


def _norm_surface(value: Any) -> str:
    """Normalize a table/column/value surface form before matching."""
    text = _norm_text_for_match(value).strip("'\"` ").lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _surface_in_text(term: str, text: str) -> bool:
    """Return whether a normalized surface term appears in text."""
    term_norm = _norm_surface(term)
    text_norm = _norm_surface(text)
    if not term_norm:
        return False
    if re.fullmatch(r"[a-z0-9_]+", term_norm):
        return re.search(r"(?<![a-z0-9_])" + re.escape(term_norm) + r"(?![a-z0-9_])", text_norm) is not None
    return term_norm in text_norm


class PrivacyProtectionAgent:
    def __init__(
        self,
        base_url: str,
        model_name: str,
        api_key: str = "EMPTY",
        db_base_dir: str = "",
        log_dir: str = "case_logs",
        request_timeout: float = 120.0,
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.client = OpenAI(api_key=api_key, base_url=base_url, timeout=request_timeout)
        self.model_name = model_name
        self.temperature = 0
        self.db_base_dir = db_base_dir
        self.log_dir = log_dir
        self.request_timeout = request_timeout

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


    def _tool_query_rewrite(self, db_schema: str, query_to_rewrite: str, rewrite_instruction: str = "", logger=None, token_usage=None, latency_stats=None) -> str:
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
        call_start = time.perf_counter()
        response = rewrite_client.chat.completions.create(model=model_name, messages=messages, temperature=self.temperature)
        _record_llm_latency(latency_stats, "query_rewrite", time.perf_counter() - call_start)
        _record_response_usage(token_usage, "query_rewrite", response, include_in_sql_generation_total=True)
        ans = response.choices[0].message.content.strip()
        return ans

    def _tool_entity_extract(self, db_schema: str, query_to_extract: str, logger=None, token_usage=None, latency_stats=None) -> str:
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_extract}"
        messages = get_entity_extract_template(input_str)
        entity_extract_client = OpenAI(
            api_key=os.getenv("ENTITY_EXTRACT_API_KEY", "EMPTY"),
            base_url=os.getenv("ENTITY_EXTRACT_BASE_URL", "http://8.130.67.139:8036/v1"),
            timeout=self.request_timeout,
        )
        model_name = os.getenv("ENTITY_EXTRACT_MODEL_NAME", str(_RELEASE_ROOT / 'LlamaFactory/saves/entity_extract_v1/checkpoint-25'))
        call_start = time.perf_counter()
        response = entity_extract_client.chat.completions.create(model=model_name, messages=messages, temperature=self.temperature)
        _record_llm_latency(latency_stats, "entity_extract", time.perf_counter() - call_start)
        _record_response_usage(token_usage, "entity_extract", response, include_in_sql_generation_total=True)
        response_text = response.choices[0].message.content.strip()
        match = re.search(r'\[\s*\{.*?\}\s*\]', response_text, re.DOTALL)
        return match.group(0) if match else "[]"
    
    def _tool_sql_generate(self, masked_query, masked_schema_str, logger=None, token_usage=None, latency_stats=None):
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
        call_start = time.perf_counter()
        response = sql_generate_client.chat.completions.create(model=model_name, messages=messages, temperature=self.temperature)
        _record_llm_latency(latency_stats, "sql_generate", time.perf_counter() - call_start)
        _record_response_usage(token_usage, "sql_generate", response, include_in_sql_generation_total=True)
        response_text = response.choices[0].message.content.strip()
        
        return response_text

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
        case_start = time.perf_counter()
        
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
        final_question = original_question  # 默认最终问题为原始问题
        token_usage = _new_token_usage_container()
        latency_stats = _new_latency_container()
        
        for iteration in range(20):
            log_to_file(f"\n{'='*20} [Round {iteration + 1}] {'='*20}")
            
            # ==========================================================
            # 1. API 请求环节：强制关闭原生解析，绕过服务端 400 报错
            # ==========================================================
            max_try = 3
            response = None
            while max_try > 0:
                try:
                    call_start = time.perf_counter()
                    response = self.client.chat.completions.create(
                        model=self.model_name, 
                        messages=messages, 
                        tools=self.tools, 
                        temperature=self.temperature
                    )
                    _record_llm_latency(latency_stats, "agent", time.perf_counter() - call_start)
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
                    log_to_file(f"⚠️ 工具解析失败: {e}\n原文: {match.group(1)}")

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
                    tool_call_id = tc_data["id"]
                    log_args_str = json.dumps(args, ensure_ascii=False, indent=2)
                    
                    log_to_file(f"🛠️ [Action - Tool Call]: {func_name}")
                    log_to_file(f"   [Action - Arguments]: {log_args_str}\n")
                    
                    # --- 执行你的工具逻辑 ---
                    tool_start = time.perf_counter()
                    try:
                        if func_name == "query_rewrite": 
                            result = self._tool_query_rewrite(db_schema, args.get('query_to_rewrite', ''), args.get('rewrite_instruction', ''), logger=log_to_file, token_usage=token_usage, latency_stats=latency_stats)
                            if isinstance(result, str) and result.strip():
                                final_question = result.strip()
                        elif func_name == "entity_extract": 
                            result = self._tool_entity_extract(db_schema, args.get('query_to_extract', ''), logger=log_to_file, token_usage=token_usage, latency_stats=latency_stats)
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
                            masked_schema = args.get('masked_schema_str', '')
                            masked_question = args.get('masked_query', '')
                            result = self._tool_sql_generate(args.get('masked_query', ''), args.get('masked_schema_str', ''), logger=log_to_file, token_usage=token_usage, latency_stats=latency_stats)
                        elif func_name == "sql_unmask":
                            masked_sql = args.get('masked_sql', '')
                            unmask_dict = args.get('unmask_dict', {})
                            result = self._tool_sql_unmask(args.get('masked_sql', ''), unmask_dict)
                        elif func_name == "execute_sql_check":
                            result = self._tool_execute_sql_check(args.get('unmasked_sql', ''), db_path)
                        else: 
                            result = f"Error: Unknown tool {func_name}"
                    except Exception as tool_error:
                        result = f"Tool Execution Error in {func_name}: {type(tool_error).__name__}: {tool_error}"
                        log_to_file(traceback.format_exc())
                    finally:
                        _record_tool_latency(latency_stats, func_name, time.perf_counter() - tool_start)
                    
                    # 格式化结果
                    tool_content = json.dumps(result, ensure_ascii=False) if isinstance(result, (dict, list)) else str(result)
                    log_to_file(f"👀 [Observation - Result]:\n{tool_content}\n")
                    
                    # 追加原生的 tool 角色消息
                    messages.append({"role": "tool", "tool_call_id": tool_call_id, "name": func_name, "content": tool_content})

            else:
                # 如果代码走到这里，说明没有匹配到 <tool_call>，代表任务结束或直接作答
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

                latency_stats["case_latency_sec"] = time.perf_counter() - case_start
                log_to_file(f"[LATENCY]: {json.dumps(latency_stats, ensure_ascii=False)}\n")
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
                    "token_usage": token_usage,
                    "latency_stats": latency_stats,
                }
        latency_stats["case_latency_sec"] = time.perf_counter() - case_start
        log_to_file(f"\n{'='*30} [CASE TIMEOUT] {'='*30}")
        log_to_file("Agent exceeded maximum iterations.")
        log_to_file(f"[LATENCY]: {json.dumps(latency_stats, ensure_ascii=False)}\n")
        return {
            "result_text": '{"error": "Agent timeout"}',
            "final_question": final_question,
            "masked_question": masked_question,
            "masked_schema": masked_schema,
            "masked_sql": masked_sql,
            "unmask_dict": unmask_dict,
            "masked_terms": masked_terms,
            "mask_mapping": mask_mapping,
            "token_usage": token_usage,
            "latency_stats": latency_stats,
        }


# ==========================================
# New Module: Independently extracted single-thread processing function
# ==========================================
def _select_agent_for_item(agent_or_agents: Union[PrivacyProtectionAgent, Sequence[PrivacyProtectionAgent]], idx: int) -> PrivacyProtectionAgent:
    if isinstance(agent_or_agents, Sequence) and not isinstance(agent_or_agents, PrivacyProtectionAgent):
        if not agent_or_agents:
            raise ValueError("agent pool is empty")
        return agent_or_agents[idx % len(agent_or_agents)]
    return agent_or_agents


def process_single_item(idx, item, agent: Union[PrivacyProtectionAgent, Sequence[PrivacyProtectionAgent]], total_num):
    case_id = str(item.get("question_id", f"idx_{idx}"))
    db_id = item.get('db_id', 'unknown_db')
    gold_sql = item.get('gold_sql', '')

    # For full BIRD-dev evaluation, use the original BIRD question_id as the
    # evaluation key. If question_id is absent, fall back to the enumerate index.
    eval_original_idx = item.get("question_id", idx)
    eval_original_idx = str(eval_original_idx)
    
    case_wall_start = time.perf_counter()
    logging.info(f"⏳ Starting to process case [{idx+1}/{total_num}] - {case_id}, eval_original_idx={eval_original_idx}")
    try:
        selected_agent = _select_agent_for_item(agent, idx)
        process_res = selected_agent.process(item, case_id=case_id)
        result_text = process_res.get("result_text", "")
        final_question = process_res.get("final_question", item.get("question", ""))
        masked_question = process_res.get("masked_question", "")
        pred_sql = find_result(result_text)
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
            "token_usage": _new_token_usage_container(),
            "latency_stats": _new_latency_container(),
        }
        process_res["latency_stats"]["case_latency_sec"] = time.perf_counter() - case_wall_start
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
        "token_usage": process_res.get("token_usage", _new_token_usage_container()),
        "latency_stats": process_res.get("latency_stats", _new_latency_container()),
        "latency_sec": (process_res.get("latency_stats") or {}).get("case_latency_sec", 0.0),
        "gold_sql": gold_sql,
        "result_text": result_text,
        "pred_sql": pred_sql,
    }

# ==========================================
# Core logic of the evaluation module
# ==========================================
def eval_exec(predicted_sql_path, db_root_path, data_mode, ground_truth_path, num_cpus, mode_gt, mode_predict, diff_json_path, meta_time_out, output_dir, inference_data_name):
    if 'spider' in inference_data_name:
        predicted_sql_path = predicted_sql_path.replace('json', 'sql')
        # Spider path replacement logic
        db_root_path = '/root/data/nl2sql/spider/database'
        if 'syn' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_syn_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        elif 'real' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_real_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        elif 'test' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_tables.json'
        elif 'dev' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        else:
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        
        eval_cmd = f'''nohup python spider_eval/evaluation.py \
        --gold {ground_truth_path} \
        --pred {predicted_sql_path} \
        --etype all \
        --db {db_root_path} \
        --table {table_path} > {output_dir}/eval.log 2>&1 &
        ''' 
    else:
        # BIRD logic
        eval_cmd = f'''python bird_eval/evaluation.py \
    --db_root_path {db_root_path} \
    --predicted_sql_path {predicted_sql_path} \
    --data_mode {data_mode} \
    --ground_truth_path {ground_truth_path} \
    --num_cpus {num_cpus} \
    --mode_gt {mode_gt} \
    --mode_predict {mode_predict} \
    --diff_json_path {diff_json_path} \
    --meta_time_out {meta_time_out} \
    --output_dir {output_dir}
    ''' 
    logging.info(f"🚀 Starting to execute the evaluation script...\n{eval_cmd}")
    subprocess.run(eval_cmd, shell=True, check=True)

def run_evaluation(
    agent: Union[PrivacyProtectionAgent, Sequence[PrivacyProtectionAgent]],
    data_path: str,
    db_root_path: str,
    output_dir: str,
    inference_data_name: str,
    diff_json_path: str,
    data_mode: str,
    ground_truth_path: str,
    max_workers: int = 0,
    case_log_dir: str = None,
    bird_eval_num_cpus: int = 4,
    workers_per_replica: int = 8,
):
    """
    Use Agent to perform multi-threaded concurrent inference on the dataset, extract SQL, and run tests
    """
    logging.info(f"Loading dataset: {data_path}")
    # Evaluate the full input dataset directly. No subset filtering and no
    # privacy-specific metrics are computed in this SQL-only evaluator.
    data = read_data(data_path)
    total_data = len(data)
    if total_data == 0:
        raise ValueError(f"No data rows found for evaluation: {data_path}")
    logging.info(f"Loaded {total_data} rows for full-dataset SQL evaluation.")
    
    # Build output path for predictions and eval
    reasoning_file_dir = os.path.join(output_dir, inference_data_name, "agent_eval")
    os.makedirs(reasoning_file_dir, exist_ok=True)
    agent_pool = list(agent) if isinstance(agent, Sequence) and not isinstance(agent, PrivacyProtectionAgent) else [agent]
    shared_case_log_dir = case_log_dir or os.path.join(reasoning_file_dir, "case_logs")
    for replica_agent in agent_pool:
        replica_agent.db_base_dir = db_root_path
        replica_agent.log_dir = shared_case_log_dir
    os.makedirs(shared_case_log_dir, exist_ok=True)

    if max_workers <= 0:
        max_workers = max(1, len(agent_pool) * max(int(workers_per_replica or 1), 1))
    
    pred_sql_path = os.path.join(reasoning_file_dir, 'predict.sql')
    json_result_path = os.path.join(reasoning_file_dir, 'predict.json')
    all_res_path = os.path.join(reasoning_file_dir, 'all_res.json')
    token_usage_path = os.path.join(reasoning_file_dir, 'token_usage.json')
    latency_path = os.path.join(reasoning_file_dir, 'latency.json')
    metrics_path = os.path.join(reasoning_file_dir, 'metrics.json')

    results_list = []
    logging.info(f"⚡ Starting multi-threaded concurrent processing, max workers: {max_workers}, agent replicas: {len(agent_pool)}")
    inference_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_item, idx, item, agent_pool, total_data): idx 
            for idx, item in enumerate(data)
        }
        for future in as_completed(futures):
            res = future.result()
            results_list.append(res)
           

    inference_wall_sec = time.perf_counter() - inference_start
    results_list.sort(key=lambda x: x["idx"])

    all_res = []
    bird_eval_dic = {}
    
    with open(pred_sql_path, 'w', encoding='utf-8') as f_sql:
        for res in results_list:
            f_sql.write(res["pred_sql"] + '\n')
            # Use original BIRD dev question_id as predict.json key.
            eval_key = str(res.get("eval_original_idx", res["idx"]))
            bird_eval_dic[eval_key] = res["pred_sql"] + '\t----- bird -----\t' + res["db_id"]
            
            # 追加新增的 final_question 和 masked_question
            all_res.append({
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
                'token_usage': res.get("token_usage", {}),
                'total_tokens': (res.get("token_usage", {}).get("sql_generation_total", {}) or {}).get("total_tokens", 0),
                'prompt_tokens': (res.get("token_usage", {}).get("sql_generation_total", {}) or {}).get("prompt_tokens", 0),
                'completion_tokens': (res.get("token_usage", {}).get("sql_generation_total", {}) or {}).get("completion_tokens", 0),
                'latency_sec': res.get("latency_sec", 0.0),
                'latency_stats': res.get("latency_stats", {}),
                'output': res["result_text"],
                'pred_sql': res["pred_sql"],
                'gold_sql': res["gold_sql"],
            })

    # Save JSON
    with open(json_result_path, 'w', encoding='utf-8') as f_json:
        json.dump(bird_eval_dic, f_json, indent=4)
        
    with open(all_res_path, 'w', encoding='utf-8') as f_all:
        json.dump(all_res, f_all, ensure_ascii=False, indent=4)

    token_usage_report = _aggregate_token_usage(results_list)
    with open(token_usage_path, 'w', encoding='utf-8') as f_token:
        json.dump(token_usage_report, f_token, ensure_ascii=False, indent=2)
    latency_report = _aggregate_latency_stats(
        results_list,
        inference_wall_sec=inference_wall_sec,
        max_workers=max_workers,
        num_agent_replicas=len(agent_pool),
    )
    with open(latency_path, 'w', encoding='utf-8') as f_latency:
        json.dump(latency_report, f_latency, ensure_ascii=False, indent=2)

    with open(metrics_path, 'w', encoding='utf-8') as f_metrics:
        json.dump({"token_usage": token_usage_report, "latency": latency_report}, f_metrics, ensure_ascii=False, indent=2)

    logging.info(
        "Token/latency aggregate: "
        + json.dumps({
            "avg_total_tokens_per_sql_generation": token_usage_report["average_total_tokens_per_sql_generation"],
            "total_tokens_for_sql_generation": token_usage_report["total_tokens_for_sql_generation"],
            "avg_case_latency_sec": latency_report["case_latency_sec"]["avg_sec_per_case"],
            "p95_case_latency_sec": latency_report["case_latency_sec"]["p95_sec_per_case"],
            "throughput_cases_per_sec": latency_report["throughput_cases_per_sec"],
        }, ensure_ascii=False)
    )

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
    replicas: int = 1
    replica_gpus: List[str] = field(default_factory=list)
    replica_ports: List[int] = field(default_factory=list)


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
    tensor_parallel_size: int = 1,
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
        "tp": tensor_parallel_size,
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
            "--tensor-parallel-size {tp} "
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
            "--tp={tp} "
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




def allocate_gpu_slots_for_checkpoints(
    checkpoints: Sequence[str],
    gpus: Sequence[str],
    base_port: int,
    replicas_per_checkpoint: int = 0,
    max_replicas_per_checkpoint: int = 0,
) -> List[Tuple[str, List[Tuple[str, int]]]]:
    """
    Assign GPUs and ports to checkpoints.

    Default behavior (replicas_per_checkpoint=0) uses all available GPUs as
    single-GPU replicas and distributes them as evenly as possible across
    checkpoints. With 3 checkpoints and 8 GPUs, this produces 3/3/2 replicas.
    Set --replicas_per_checkpoint 1 to keep the old one-GPU-per-checkpoint mode.
    """
    checkpoints = list(checkpoints)
    gpus = [str(g).strip() for g in gpus if str(g).strip()]
    if not checkpoints:
        return []
    if not gpus:
        raise ValueError("No GPU ids provided.")

    if replicas_per_checkpoint > 0:
        counts = [int(replicas_per_checkpoint)] * len(checkpoints)
        if sum(counts) > len(gpus):
            raise ValueError(
                f"replicas_per_checkpoint={replicas_per_checkpoint} requires {sum(counts)} GPUs "
                f"for {len(checkpoints)} checkpoints, but only {len(gpus)} GPU ids were provided."
            )
    else:
        if len(gpus) < len(checkpoints):
            # Not enough GPUs to evaluate every checkpoint concurrently. Keep one
            # replica per checkpoint; the caller's max_parallel controls queuing.
            counts = [1] * len(checkpoints)
            logging.warning(
                "Fewer GPUs than checkpoints. Falling back to one replica per checkpoint; "
                "some jobs will wait for a GPU slot."
            )
        else:
            base = len(gpus) // len(checkpoints)
            remainder = len(gpus) % len(checkpoints)
            counts = [base + (1 if i < remainder else 0) for i in range(len(checkpoints))]

    if max_replicas_per_checkpoint and max_replicas_per_checkpoint > 0:
        counts = [min(c, int(max_replicas_per_checkpoint)) for c in counts]

    assignments = []
    gpu_idx = 0
    port = int(base_port)
    for ckpt, count in zip(checkpoints, counts):
        slots = []
        for _ in range(count):
            if gpu_idx >= len(gpus):
                break
            slots.append((gpus[gpu_idx], port))
            gpu_idx += 1
            port += 1
        if not slots:
            # This only happens when there are fewer GPUs than checkpoints. The
            # queued fallback in run_multi_checkpoint_evaluation will handle it.
            continue
        assignments.append((ckpt, slots))
    return assignments

def evaluate_one_checkpoint(
    checkpoint_path: str,
    gpu_slots: List[Tuple[str, int]],
    args: argparse.Namespace,
) -> CheckpointJobResult:
    ckpt_name = os.path.basename(os.path.normpath(checkpoint_path))
    ckpt_output_dir = os.path.join(args.output_dir, ckpt_name)
    server_log_dir = os.path.join(ckpt_output_dir, "server_logs")
    os.makedirs(server_log_dir, exist_ok=True)

    start = time.time()
    server_procs: List[subprocess.Popen] = []
    server_log_files = []
    agents: List[PrivacyProtectionAgent] = []
    replica_gpus = [str(gpu) for gpu, _ in gpu_slots]
    replica_ports = [int(port) for _, port in gpu_slots]

    try:
        api_key = args.agent_api_key
        if not gpu_slots:
            raise ValueError(f"No GPU slots assigned for checkpoint {checkpoint_path}")

        replica_infos = []
        for replica_idx, (gpu, port) in enumerate(gpu_slots):
            base_url = f"http://{args.host}:{port}/v1"
            server_cmd = make_server_command(
                backend=args.server_backend,
                model_path=checkpoint_path,
                host=args.host,
                port=port,
                gpu=gpu,
                tensor_parallel_size=args.server_tp,
                template=args.server_cmd_template,
                chat_template=args.chat_template,
                extra_args=args.server_extra_args,
            )
            replica_infos.append((replica_idx, str(gpu), int(port), base_url, server_cmd))

        if not args.no_auto_deploy:
            # Launch all replicas first, then wait for readiness. This reduces
            # startup wall time compared with launching/waiting sequentially.
            for replica_idx, gpu, port, base_url, server_cmd in replica_infos:
                env = os.environ.copy()
                env["CUDA_VISIBLE_DEVICES"] = str(gpu)
                env.setdefault("TOKENIZERS_PARALLELISM", "false")
                log_path = os.path.join(server_log_dir, f"server_replica{replica_idx}_gpu{gpu}_port{port}.log")
                logging.info(f"[{ckpt_name}] launching replica {replica_idx} on GPU {gpu}, port {port}: {server_cmd}")
                log_f = open(log_path, "w", encoding="utf-8")
                server_log_files.append(log_f)
                server_procs.append(subprocess.Popen(
                    server_cmd,
                    shell=True,
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    env=env,
                    preexec_fn=os.setsid,
                ))
        else:
            logging.info(f"[{ckpt_name}] auto deploy disabled; using existing replicas on ports {replica_ports}")

        for replica_idx, gpu, port, base_url, _ in replica_infos:
            wait_for_openai_server(base_url, api_key, args.server_startup_timeout)
            served_model_name = get_served_model_id(
                base_url=base_url,
                api_key=api_key,
                fallback_model_name=checkpoint_path,
                timeout=min(args.request_timeout, 30.0),
            )
            logging.info(f"[{ckpt_name}] replica {replica_idx} ready: gpu={gpu}, port={port}, model_id={served_model_name}")
            agents.append(PrivacyProtectionAgent(
                base_url=base_url,
                model_name=served_model_name,
                api_key=api_key,
                db_base_dir=args.db_base_dir,
                log_dir=os.path.join(ckpt_output_dir, "case_logs"),
                request_timeout=args.request_timeout,
            ))

        for inference_data_name in args.inference_data_names:
            data_path = os.path.join(args.eval_data_base_dir, f"{inference_data_name}.jsonl")
            logging.info(f"[{ckpt_name}] evaluating dataset: {inference_data_name}; replicas={len(agents)}; gpus={replica_gpus}")
            run_evaluation(
                agent=agents,
                data_path=data_path,
                db_root_path=args.db_base_dir,
                output_dir=ckpt_output_dir,
                inference_data_name=inference_data_name,
                diff_json_path=args.diff_json_path,
                data_mode=args.data_mode,
                ground_truth_path=args.ground_truth_path,
                max_workers=args.per_checkpoint_workers,
                case_log_dir=os.path.join(ckpt_output_dir, inference_data_name, "agent_eval", "case_logs"),
                bird_eval_num_cpus=args.bird_eval_num_cpus,
                workers_per_replica=args.workers_per_replica,
            )

        return CheckpointJobResult(
            checkpoint=checkpoint_path,
            checkpoint_name=ckpt_name,
            gpu=",".join(replica_gpus),
            port=replica_ports[0],
            output_dir=ckpt_output_dir,
            success=True,
            elapsed_sec=time.time() - start,
            replicas=len(replica_gpus),
            replica_gpus=replica_gpus,
            replica_ports=replica_ports,
        )
    except Exception as e:
        logging.error(f"[{ckpt_name}] failed: {type(e).__name__}: {e}")
        logging.error(traceback.format_exc())
        return CheckpointJobResult(
            checkpoint=checkpoint_path,
            checkpoint_name=ckpt_name,
            gpu=",".join(replica_gpus),
            port=replica_ports[0] if replica_ports else -1,
            output_dir=ckpt_output_dir,
            success=False,
            error=f"{type(e).__name__}: {e}",
            elapsed_sec=time.time() - start,
            replicas=len(replica_gpus),
            replica_gpus=replica_gpus,
            replica_ports=replica_ports,
        )
    finally:
        if server_procs and not args.keep_servers:
            logging.info(f"[{ckpt_name}] terminating {len(server_procs)} server replica(s): gpus={replica_gpus}, ports={replica_ports}")
            for proc in server_procs:
                terminate_process_group(proc)
        for log_f in server_log_files:
            try:
                log_f.close()
            except Exception:
                pass

def run_multi_checkpoint_evaluation(args: argparse.Namespace) -> List[CheckpointJobResult]:
    checkpoints = args.checkpoints or discover_checkpoints(args.checkpoint_root, args.checkpoint_pattern)
    if args.limit_checkpoints > 0:
        checkpoints = checkpoints[:args.limit_checkpoints]

    gpus = [g.strip() for g in args.gpus.split(",") if g.strip()]
    if not gpus:
        raise ValueError("No GPU ids provided. Example: --gpus 0,1,2,3,4,5,6,7")

    os.makedirs(args.output_dir, exist_ok=True)
    summary_path = os.path.join(args.output_dir, "multi_checkpoint_summary.json")

    results: List[CheckpointJobResult] = []

    if args.replica_schedule == "sequential_full_gpu":
        if args.no_auto_deploy:
            raise ValueError("--replica_schedule sequential_full_gpu requires auto deployment; --no_auto_deploy would reuse the same existing servers for different checkpoints.")
        if args.keep_servers and len(checkpoints) > 1:
            raise ValueError("--keep_servers is incompatible with sequential_full_gpu for multiple checkpoints because GPUs/ports must be reused by the next checkpoint.")

        # Fastest schedule when checkpoints < GPUs: evaluate one checkpoint at a
        # time, launch single-GPU replicas, shard the dataset across replicas,
        # then move to the next checkpoint. By default this uses all GPUs, but
        # --replicas_per_checkpoint / --max_replicas_per_checkpoint can cap it.
        replica_count = len(gpus)
        if args.replicas_per_checkpoint and args.replicas_per_checkpoint > 0:
            replica_count = min(int(args.replicas_per_checkpoint), len(gpus))
        if args.max_replicas_per_checkpoint and args.max_replicas_per_checkpoint > 0:
            replica_count = min(replica_count, int(args.max_replicas_per_checkpoint))
        selected_gpus = gpus[:replica_count]
        slots = [(gpu, args.base_port + i) for i, gpu in enumerate(selected_gpus)]
        logging.info(
            "Found %d checkpoint(s), %d GPU(s). Using sequential_full_gpu: each checkpoint gets slots=%s",
            len(checkpoints), len(gpus), slots,
        )
        for ckpt in checkpoints:
            res = evaluate_one_checkpoint(ckpt, slots, args)
            results.append(res)
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump([asdict(x) for x in sorted(results, key=lambda r: _checkpoint_sort_key(r.checkpoint))], f, ensure_ascii=False, indent=2)
            status = "OK" if res.success else "FAILED"
            logging.info(f"[{res.checkpoint_name}] {status}; replicas={res.replicas}; elapsed={res.elapsed_sec:.1f}s; output={res.output_dir}")

    # Alternative schedule: evaluate all checkpoints concurrently, splitting
    # GPUs across checkpoints. With 3 checkpoints and 8 GPUs this is 3/3/2.
    elif len(gpus) >= len(checkpoints):
        assignments = allocate_gpu_slots_for_checkpoints(
            checkpoints=checkpoints,
            gpus=gpus,
            base_port=args.base_port,
            replicas_per_checkpoint=args.replicas_per_checkpoint,
            max_replicas_per_checkpoint=args.max_replicas_per_checkpoint,
        )
        max_parallel = min(args.max_parallel, len(assignments))
        logging.info(
            "Found %d checkpoint(s), %d GPU(s). Replica assignments: %s",
            len(checkpoints), len(gpus),
            {os.path.basename(c): slots for c, slots in assignments},
        )

        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {executor.submit(evaluate_one_checkpoint, ckpt, slots, args): ckpt for ckpt, slots in assignments}
            for future in as_completed(futures):
                res = future.result()
                results.append(res)
                with open(summary_path, "w", encoding="utf-8") as f:
                    json.dump([asdict(x) for x in sorted(results, key=lambda r: _checkpoint_sort_key(r.checkpoint))], f, ensure_ascii=False, indent=2)
                status = "OK" if res.success else "FAILED"
                logging.info(f"[{res.checkpoint_name}] {status}; replicas={res.replicas}; elapsed={res.elapsed_sec:.1f}s; output={res.output_dir}")
    else:
        # General fallback: fewer GPUs than checkpoints. Preserve queued
        # one-GPU-per-checkpoint behavior.
        max_parallel = min(args.max_parallel, len(gpus), len(checkpoints))
        slots = Queue()
        for i, gpu in enumerate(gpus[:max_parallel]):
            slots.put((gpu, args.base_port + i))

        logging.info(f"Found {len(checkpoints)} checkpoint(s). Running up to {max_parallel} in parallel on GPUs {gpus[:max_parallel]}.")

        def _job_wrapper(ckpt: str):
            gpu, port = slots.get()
            try:
                return evaluate_one_checkpoint(ckpt, [(gpu, port)], args)
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
    parser.add_argument("--replicas_per_checkpoint", type=int, default=0, help="0 means auto-distribute all GPUs across checkpoints; 1 keeps old one-GPU-per-checkpoint behavior.")
    parser.add_argument("--max_replicas_per_checkpoint", type=int, default=0, help="Optional cap for auto replicas; 0 means no cap.")
    parser.add_argument(
        "--replica_schedule",
        choices=["sequential_full_gpu", "concurrent_balanced"],
        default="sequential_full_gpu",
        help=(
            "sequential_full_gpu runs one checkpoint at a time with all GPUs as replicas; "
            "this is usually fastest when checkpoints < GPUs. concurrent_balanced evaluates "
            "all checkpoints at once and splits GPUs across checkpoints."
        ),
    )
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
            "lmdeploy serve api_server --log-level=INFO --server-port {port} --tp={tp} {model_path}"
        ),
    )
    parser.add_argument("--server_extra_args", default="")
    parser.add_argument("--server_tp", type=int, default=1, help="Tensor parallel size per replica. Keep 1 for replica-based throughput; use >1 only when a single model needs multiple GPUs.")
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
    # BIRD / Spider external evaluator settings
    parser.add_argument("--db_base_dir", default=str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1'))
    parser.add_argument("--diff_json_path", default="/mnt/wh_intern/MXY/paper/rl/ours/open-r1/data/bird/diff.json")
    parser.add_argument("--data_mode", default="dev")
    parser.add_argument("--ground_truth_path", default="/mnt/wh_intern/MXY/paper/rl/ours/CHESS/data/dev/dev.sql")
    parser.add_argument("--bird_eval_num_cpus", type=int, default=4)

    # Per-checkpoint in-dataset concurrency. Do not leave this too high when 8
    # model servers run simultaneously; otherwise the local servers and auxiliary
    # rewrite/extract/generate services may be overloaded.
    parser.add_argument("--per_checkpoint_workers", type=int, default=0, help="0 means auto = replicas * workers_per_replica.")
    parser.add_argument("--workers_per_replica", type=int, default=8, help="Used only when --per_checkpoint_workers is 0.")

    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    run_multi_checkpoint_evaluation(cli_args)
