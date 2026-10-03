#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Direct DeepSeek Text-to-SQL evaluator.

This script evaluates the baseline where the full original DB schema and the
original natural-language query are sent directly to a cloud DeepSeek-compatible
OpenAI API. It skips the privacy agent, local checkpoint deployment, masking,
unmasking, and tool-calling loop.

Outputs per dataset:
  <output_dir>/<inference_data_name>/direct_deepseek_eval/
    - predict.sql
    - predict.json
    - all_res.json
    - token_usage.json
    - latency.json
    - metrics.json
    - eval.log / official evaluator outputs

Example:
python eval_direct_deepseek_sql.py \
  --eval_data_base_dir /mnt/wh_intern/MXY/paper/privacy-agent/agent_data_create/data \
  --db_base_dir /root/data/nl2sql/spider/database \
  --inference_data_names spider_dev_sc \
  --ground_truth_path /mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql \
  --max_workers 50
"""

# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))


import argparse
import json
import logging
import os
import re
import subprocess
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List

from openai import OpenAI

try:
    # Reuse your existing SQL-generation prompt if the script is run inside the
    # original project directory.
    from template.sql_generate_template import get_sql_generate_template  # type: ignore
except Exception:
    get_sql_generate_template = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SQL_GENERATE_DEFAULT_BASE_URL = "https://api.deepseek.com/v1"
SQL_GENERATE_DEFAULT_MODEL_NAME = "deepseek-v4-flash"
SQL_GENERATE_DEFAULT_API_KEY = "EMPTY"


def get_sql_generate_api_config(args: argparse.Namespace) -> Dict[str, str]:
    return {
        "api_key": args.api_key or os.getenv("SQL_GENERATE_API_KEY", SQL_GENERATE_DEFAULT_API_KEY),
        "base_url": args.base_url or os.getenv("SQL_GENERATE_BASE_URL", SQL_GENERATE_DEFAULT_BASE_URL),
        "model_name": args.model_name or os.getenv("SQL_GENERATE_MODEL_NAME", SQL_GENERATE_DEFAULT_MODEL_NAME),
    }


def clean_space(sql: str) -> str:
    sql = re.sub(r"\s*\)", ")", sql)
    for fn in ["min", "max", "sum", "avg", "count", "MIN", "MAX", "SUM", "AVG", "COUNT"]:
        sql = re.sub(rf"{fn}\s*\(\s*", f"{fn}(", sql)
    sql = re.sub(r"\(\s*", "(", sql)
    sql = re.sub(r" +", " ", sql)
    sql = re.sub(r"\s*,", ",", sql)
    return sql.strip()


def find_result(text: str) -> str:
    if not text:
        return ""

    # Prefer fenced SQL.
    if "```sql" in text.lower():
        pattern = r"```sql\s*(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
        if matches:
            return clean_space(matches[-1].replace("\n", " ").strip())

    # Then any fenced code block.
    if "```" in text:
        pattern = r"```\s*(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        if matches:
            return clean_space(matches[-1].replace("\n", " ").strip())

    # Remove common prefixes if the model answers in prose.
    text = text.strip()
    text = re.sub(r"^\s*(SQL|Answer|Final SQL)\s*:\s*", "", text, flags=re.IGNORECASE)
    return clean_space(text.replace("\n", " ").strip())


def read_data(path: str, encoding: str = "utf-8") -> List[Dict[str, Any]]:
    if path.endswith(".jsonl"):
        with open(path, "r", encoding=encoding) as f:
            data = [json.loads(line) for line in f if line.strip()]
    else:
        with open(path, "r", encoding=encoding) as f:
            data = json.load(f)
    return sorted(data, key=lambda x: int(x.get("question_id", 0)))


def usage_to_dict(response: Any) -> Dict[str, int]:
    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, dict):
        usage = response.get("usage")
    if usage is None:
        return {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "num_calls": 1,
            "num_missing_usage": 1,
        }

    def _get(name: str) -> int:
        if isinstance(usage, dict):
            value = usage.get(name, 0)
        else:
            value = getattr(usage, name, 0)
        try:
            return int(value or 0)
        except Exception:
            return 0

    prompt_tokens = _get("prompt_tokens")
    completion_tokens = _get("completion_tokens")
    total_tokens = _get("total_tokens") or prompt_tokens + completion_tokens
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "num_calls": 1,
        "num_missing_usage": 0,
    }


def build_messages(item: Dict[str, Any], prompt_style: str = "project_template") -> List[Dict[str, str]]:
    schema = item.get("db_schema", "")
    question = item.get("question", "")
    evidence = item.get("evidence", "")
    full_question = f"{question} {evidence}".strip()

    input_str = f"- Database Schema:\n{schema}\n- User's Question:\n{full_question}"

    if prompt_style == "project_template" and get_sql_generate_template is not None:
        return get_sql_generate_template(input_str)

    system_prompt = (
        "You are an expert Text-to-SQL system. Given a database schema and a user question, "
        "write one executable SQLite SQL query that answers the question. "
        "Return only the SQL query. Do not include explanations."
    )
    user_prompt = (
        f"Database Schema:\n{schema}\n\n"
        f"User Question:\n{full_question}\n\n"
        "Output only the SQL query."
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def call_deepseek_for_one(
    idx: int,
    item: Dict[str, Any],
    total_num: int,
    api_cfg: Dict[str, str],
    request_timeout: float,
    max_retries: int,
    retry_sleep: float,
    prompt_style: str,
    temperature: float,
) -> Dict[str, Any]:
    case_id = str(item.get("question_id", f"idx_{idx}"))
    eval_original_idx = str(item.get("question_id", idx))
    db_id = item.get("db_id", "unknown_db")
    start = time.time()

    logging.info(f"Starting direct DeepSeek case [{idx + 1}/{total_num}] - {case_id}")

    result_text = ""
    pred_sql = ""
    error = ""
    token_usage = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "num_calls": 0,
        "num_missing_usage": 0,
    }

    try:
        client = OpenAI(
            api_key=api_cfg["api_key"],
            base_url=api_cfg["base_url"],
            timeout=request_timeout,
        )
        messages = build_messages(item, prompt_style=prompt_style)

        last_err = None
        response = None
        for attempt in range(1, max_retries + 1):
            try:
                response = client.chat.completions.create(
                    model=api_cfg["model_name"],
                    messages=messages,
                    temperature=temperature,
                )
                break
            except Exception as e:
                last_err = e
                logging.warning(
                    f"Case {case_id} DeepSeek request failed, attempt {attempt}/{max_retries}: "
                    f"{type(e).__name__}: {e}"
                )
                if attempt < max_retries:
                    time.sleep(retry_sleep)

        if response is None:
            raise RuntimeError(f"DeepSeek request failed after {max_retries} retries: {last_err}")

        token_usage = usage_to_dict(response)
        result_text = response.choices[0].message.content or ""
        pred_sql = find_result(result_text)
        logging.info(f"Finished direct DeepSeek case [{idx + 1}/{total_num}] - {case_id}")
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        result_text = json.dumps(
            {"error": "direct deepseek case failed", "exception_type": type(e).__name__, "message": str(e)},
            ensure_ascii=False,
        )
        pred_sql = ""
        logging.error(f"Case failed [{idx + 1}/{total_num}] - {case_id}: {error}")
        logging.error(traceback.format_exc())

    latency_sec = time.time() - start
    return {
        "idx": idx,
        "eval_original_idx": eval_original_idx,
        "case_id": case_id,
        "db_id": db_id,
        "question": item.get("question", ""),
        "evidence": item.get("evidence", ""),
        "db_schema": item.get("db_schema", ""),
        "gold_sql": item.get("gold_sql", ""),
        "result_text": result_text,
        "pred_sql": pred_sql,
        "token_usage": token_usage,
        "latency_sec": latency_sec,
        "error": error,
    }


def aggregate_token_usage(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "num_calls": 0,
        "num_missing_usage": 0,
    }
    for res in results:
        usage = res.get("token_usage") or {}
        for key in total:
            total[key] += int(usage.get(key, 0) or 0)

    denom = max(len(results), 1)
    return {
        "num_cases": len(results),
        "definition": "Direct cloud Text-to-SQL baseline. Token usage only counts the single DeepSeek SQL-generation call per example.",
        "average_total_tokens_per_sql_generation": total["total_tokens"] / denom,
        "average_prompt_tokens_per_sql_generation": total["prompt_tokens"] / denom,
        "average_completion_tokens_per_sql_generation": total["completion_tokens"] / denom,
        "total_tokens_for_sql_generation": total["total_tokens"],
        "total_prompt_tokens_for_sql_generation": total["prompt_tokens"],
        "total_completion_tokens_for_sql_generation": total["completion_tokens"],
        "token_usage_scope": "direct_deepseek_sql_generation_only",
        "by_component": {"sql_generate": {**total}},
    }


def percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    k = (len(values) - 1) * pct / 100.0
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] * (c - k) + values[c] * (k - f)


def aggregate_latency(results: List[Dict[str, Any]], wall_sec: float, max_workers: int) -> Dict[str, Any]:
    vals = [float(res.get("latency_sec", 0.0) or 0.0) for res in results]
    return {
        "num_cases": len(results),
        "max_workers": max_workers,
        "inference_wall_sec": wall_sec,
        "throughput_cases_per_sec": len(results) / wall_sec if wall_sec > 0 else 0.0,
        "case_latency_sec": {
            "avg_sec_per_case": mean(vals) if vals else 0.0,
            "min_sec_per_case": min(vals) if vals else 0.0,
            "max_sec_per_case": max(vals) if vals else 0.0,
            "p50_sec_per_case": median(vals) if vals else 0.0,
            "p95_sec_per_case": percentile(vals, 95),
        },
    }


def eval_exec(
    predicted_sql_path: str,
    db_root_path: str,
    data_mode: str,
    ground_truth_path: str,
    num_cpus: int,
    mode_gt: str,
    mode_predict: str,
    diff_json_path: str,
    meta_time_out: int,
    output_dir: str,
    inference_data_name: str,
) -> None:
    if "spider" in inference_data_name:
        predicted_sql_path = predicted_sql_path.replace("json", "sql")
        db_root_path = "/root/data/nl2sql/spider/database"
        if "syn" in inference_data_name:
            ground_truth_path = "/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_syn_gold.sql"
            table_path = "/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json"
        elif "real" in inference_data_name:
            ground_truth_path = "/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_real_gold.sql"
            table_path = "/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json"
        elif "test" in inference_data_name:
            ground_truth_path = ground_truth_path or "/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_gold.sql"
            table_path = "/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_tables.json"
        elif "dev" in inference_data_name:
            ground_truth_path = ground_truth_path or "/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql"
            table_path = "/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json"
        else:
            table_path = "/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json"

        eval_cmd = (
            f"python spider_eval/evaluation.py "
            f"--gold {ground_truth_path} "
            f"--pred {predicted_sql_path} "
            f"--etype all "
            f"--db {db_root_path} "
            f"--table {table_path} "
            f"> {output_dir}/eval.log 2>&1"
        )
    else:
        eval_cmd = (
            f"python bird_eval/evaluation.py "
            f"--db_root_path {db_root_path} "
            f"--predicted_sql_path {predicted_sql_path} "
            f"--data_mode {data_mode} "
            f"--ground_truth_path {ground_truth_path} "
            f"--num_cpus {num_cpus} "
            f"--mode_gt {mode_gt} "
            f"--mode_predict {mode_predict} "
            f"--diff_json_path {diff_json_path} "
            f"--meta_time_out {meta_time_out} "
            f"--output_dir {output_dir}"
        )

    logging.info(f"Starting official evaluator:\n{eval_cmd}")
    subprocess.run(eval_cmd, shell=True, check=True)


def run_direct_evaluation(args: argparse.Namespace, inference_data_name: str) -> None:
    data_path = os.path.join(args.eval_data_base_dir, f"{inference_data_name}.jsonl")
    data = read_data(data_path)
    if not data:
        raise ValueError(f"No data found: {data_path}")

    output_dir = os.path.join(args.output_dir, inference_data_name, "direct_deepseek_eval")
    os.makedirs(output_dir, exist_ok=True)

    pred_sql_path = os.path.join(output_dir, "predict.sql")
    json_result_path = os.path.join(output_dir, "predict.json")
    all_res_path = os.path.join(output_dir, "all_res.json")
    token_usage_path = os.path.join(output_dir, "token_usage.json")
    latency_path = os.path.join(output_dir, "latency.json")
    metrics_path = os.path.join(output_dir, "metrics.json")

    api_cfg = get_sql_generate_api_config(args)
    logging.info(
        f"Direct DeepSeek evaluation: dataset={inference_data_name}, rows={len(data)}, "
        f"model={api_cfg['model_name']}, base_url={api_cfg['base_url']}, max_workers={args.max_workers}"
    )

    start = time.time()
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                call_deepseek_for_one,
                idx,
                item,
                len(data),
                api_cfg,
                args.request_timeout,
                args.max_retries,
                args.retry_sleep,
                args.prompt_style,
                args.temperature,
            ): idx
            for idx, item in enumerate(data)
        }
        for future in as_completed(futures):
            results.append(future.result())

    wall_sec = time.time() - start
    results.sort(key=lambda x: x["idx"])

    bird_eval_dic: Dict[str, str] = {}
    all_res: List[Dict[str, Any]] = []
    with open(pred_sql_path, "w", encoding="utf-8") as f_sql:
        for res in results:
            f_sql.write(res["pred_sql"] + "\n")
            eval_key = str(res.get("eval_original_idx", res["idx"]))
            bird_eval_dic[eval_key] = res["pred_sql"] + "\t----- bird -----\t" + res["db_id"]
            usage = res.get("token_usage", {}) or {}
            all_res.append(
                {
                    "eval_original_idx": eval_key,
                    "question_id": res.get("case_id"),
                    "case_id": res.get("case_id"),
                    "db_id": res.get("db_id", ""),
                    "input": res.get("question", ""),
                    "evidence": res.get("evidence", ""),
                    "db_schema": res.get("db_schema", ""),
                    "output": res.get("result_text", ""),
                    "pred_sql": res.get("pred_sql", ""),
                    "gold_sql": res.get("gold_sql", ""),
                    "token_usage": usage,
                    "total_tokens": usage.get("total_tokens", 0),
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                    "latency_sec": res.get("latency_sec", 0.0),
                    "error": res.get("error", ""),
                }
            )

    with open(json_result_path, "w", encoding="utf-8") as f_json:
        json.dump(bird_eval_dic, f_json, ensure_ascii=False, indent=4)
    with open(all_res_path, "w", encoding="utf-8") as f_all:
        json.dump(all_res, f_all, ensure_ascii=False, indent=2)

    token_report = aggregate_token_usage(results)
    latency_report = aggregate_latency(results, wall_sec=wall_sec, max_workers=args.max_workers)
    metrics = {
        "dataset": inference_data_name,
        "model_name": api_cfg["model_name"],
        "base_url": api_cfg["base_url"],
        "prompt_style": args.prompt_style,
        "token_usage": token_report,
        "latency": latency_report,
        "num_errors": sum(1 for r in results if r.get("error")),
    }

    with open(token_usage_path, "w", encoding="utf-8") as f:
        json.dump(token_report, f, ensure_ascii=False, indent=2)
    with open(latency_path, "w", encoding="utf-8") as f:
        json.dump(latency_report, f, ensure_ascii=False, indent=2)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    logging.info(
        "Direct DeepSeek token/latency summary: "
        + json.dumps(
            {
                "avg_total_tokens": token_report["average_total_tokens_per_sql_generation"],
                "wall_sec": latency_report["inference_wall_sec"],
                "throughput": latency_report["throughput_cases_per_sec"],
                "num_errors": metrics["num_errors"],
            },
            ensure_ascii=False,
        )
    )

    if not args.skip_eval:
        eval_exec(
            predicted_sql_path=json_result_path,
            db_root_path=args.db_base_dir,
            data_mode=args.data_mode,
            ground_truth_path=args.ground_truth_path,
            num_cpus=args.bird_eval_num_cpus,
            mode_gt="gt",
            mode_predict="gpt",
            diff_json_path=args.diff_json_path,
            meta_time_out=args.meta_time_out,
            output_dir=output_dir,
            inference_data_name=inference_data_name,
        )

    logging.info(f"Finished direct DeepSeek evaluation. Output: {output_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Direct DeepSeek Text-to-SQL baseline evaluator.")

    parser.add_argument("--eval_data_base_dir", default=str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data'))
    parser.add_argument("--output_dir", default=str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/ineference/output_direct_deepseek'))
    parser.add_argument("--inference_data_names", nargs="+", default=["bird_dev_sc"])
    parser.add_argument("--db_base_dir", default=str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1'))
    parser.add_argument("--diff_json_path", default="/mnt/wh_intern/MXY/paper/rl/ours/open-r1/data/bird/diff.json")
    parser.add_argument("--data_mode", default="dev")
    parser.add_argument("--ground_truth_path", default="/mnt/wh_intern/MXY/paper/rl/ours/CHESS/data/dev/dev.sql")
    parser.add_argument("--bird_eval_num_cpus", type=int, default=4)
    parser.add_argument("--meta_time_out", type=int, default=30)

    parser.add_argument("--base_url", default="", help="Default reads SQL_GENERATE_BASE_URL or https://api.deepseek.com/v1")
    parser.add_argument("--model_name", default="", help="Default reads SQL_GENERATE_MODEL_NAME or deepseek-v4-flash")
    parser.add_argument("--api_key", default="", help="Default reads SQL_GENERATE_API_KEY")
    parser.add_argument("--request_timeout", type=float, default=120.0)
    parser.add_argument("--max_retries", type=int, default=3)
    parser.add_argument("--retry_sleep", type=float, default=2.0)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--max_workers", type=int, default=50, help="Concurrent DeepSeek requests.")
    parser.add_argument(
        "--prompt_style",
        choices=["project_template", "simple"],
        default="project_template",
        help="project_template reuses template.sql_generate_template when available; otherwise falls back to simple.",
    )
    parser.add_argument("--skip_eval", action="store_true", help="Only generate predictions and metrics; skip official evaluator.")

    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    for name in cli_args.inference_data_names:
        run_direct_evaluation(cli_args, name)
