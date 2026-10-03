#!/usr/bin/env python3
"""DeepSeek SQL-candidate reranker for Spider.

The reranker is intentionally non-oracular: it sees only the question, schema,
optional evidence, and candidate SQL strings. It does not read gold SQL or
execution-match labels. Gold files should be used only by a separate scorer.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

from openai import OpenAI


def read_rows(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    data = json.loads(text)
    if isinstance(data, dict):
        for key in ("data", "examples", "items"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
    if not isinstance(data, list):
        raise ValueError(f"Unsupported row file: {path}")
    return data


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def load_candidate(spec: str, n: int) -> Dict[str, List[str]]:
    name, raw_path = spec.split("=", 1)
    path = Path(raw_path)
    if path.suffix == ".sql":
        sqls = path.read_text(encoding="utf-8").splitlines()
    else:
        rows = read_rows(path)
        sqls = [str(row.get("pred_sql") or row.get("predict_sql") or row.get("selected_sql_candidate") or "") for row in rows]
    if len(sqls) < n:
        sqls.extend([""] * (n - len(sqls)))
    return {"name": name, "sqls": sqls[:n]}


def usage_to_dict(response: Any) -> Dict[str, int]:
    usage = getattr(response, "usage", None)
    if usage is None:
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "num_missing_usage": 1}

    def get(name: str) -> int:
        value = getattr(usage, name, 0)
        try:
            return int(value or 0)
        except Exception:
            return 0

    prompt = get("prompt_tokens")
    completion = get("completion_tokens")
    total = get("total_tokens") or prompt + completion
    return {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total, "num_missing_usage": 0}


def normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def run_sql_summary(db_path: Path, sql: str, timeout_sec: float, max_rows: int = 8) -> Dict[str, Any]:
    if not sql.strip():
        return {"ok": False, "error": "empty sql", "rows": []}
    try:
        conn = sqlite3.connect(str(db_path))
        conn.text_factory = lambda raw: raw.decode("utf-8", errors="replace")
        started = time.monotonic()

        def stop_slow_query() -> int:
            return 1 if time.monotonic() - started > timeout_sec else 0

        conn.set_progress_handler(stop_slow_query, 10000)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchmany(max_rows + 1)
        conn.close()
        shown = [[normalize_value(v) for v in row] for row in rows[:max_rows]]
        return {"ok": True, "error": "", "rows": shown, "truncated": len(rows) > max_rows}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "rows": []}


def extract_choice(text: str, names: List[str]) -> str:
    if not text:
        return names[0]
    try:
        data = json.loads(text)
        choice = str(data.get("choice", "")).strip()
        if choice in names:
            return choice
    except Exception:
        pass
    for name in names:
        if re.search(rf"\b{re.escape(name)}\b", text):
            return name
    return names[0]


def build_prompt(row: Dict[str, Any], candidates: List[Dict[str, Any]]) -> str:
    question = row.get("input") or row.get("question") or ""
    evidence = row.get("evidence") or ""
    schema = row.get("db_schema") or ""
    cand_lines = []
    for item in candidates:
        line = f"{item['name']} SQL: {item['sql']}"
        if "execution" in item:
            line += f"\n{item['name']} execution: {json.dumps(item['execution'], ensure_ascii=False)}"
        cand_lines.append(line)
    cand_text = "\n\n".join(cand_lines)
    return f"""Choose the SQL candidate that best answers the question for the given SQLite schema.

Return only compact JSON: {{"choice":"<candidate_name>"}}

Question:
{question}

Evidence:
{evidence}

Schema:
{schema}

Candidates:
{cand_text}
"""


def choose_one(
    idx: int,
    row: Dict[str, Any],
    cand_sets: List[Dict[str, List[str]]],
    client: OpenAI,
    model: str,
    temperature: float,
    timeout: float,
    db_root: Path | None,
    sql_timeout: float,
) -> Dict[str, Any]:
    candidates = [{"name": item["name"], "sql": item["sqls"][idx]} for item in cand_sets]
    if db_root is not None:
        db_id = str(row.get("db_id", ""))
        db_path = db_root / db_id / f"{db_id}.sqlite"
        for item in candidates:
            item["execution"] = run_sql_summary(db_path, item["sql"], sql_timeout)
    started = time.time()
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a careful Text-to-SQL candidate selector. Do not invent new SQL; choose one candidate."},
                {"role": "user", "content": build_prompt(row, candidates)},
            ],
            temperature=temperature,
            timeout=timeout,
        )
        content = response.choices[0].message.content or ""
        usage = usage_to_dict(response)
        error = ""
    except Exception as exc:
        content = ""
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "num_missing_usage": 1}
        error = f"{type(exc).__name__}: {exc}"
    names = [item["name"] for item in candidates]
    choice = extract_choice(content, names)
    selected_sql = next((item["sql"] for item in candidates if item["name"] == choice), candidates[0]["sql"])
    return {
        "idx": idx,
        "choice": choice,
        "pred_sql": selected_sql,
        "raw_response": content,
        "token_usage": usage,
        "latency_sec": time.time() - started,
        "error": error,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--base-all-res", type=Path, required=True)
    parser.add_argument("--candidate", action="append", required=True, help="name=/path/to/all_res.json or .sql")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--base-url", default=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"))
    parser.add_argument("--api-key", default=os.getenv("DEEPSEEK_API_KEY", ""))
    parser.add_argument("--model", default="deepseek-chat")
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--max-workers", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--db-root", type=Path, default=None)
    parser.add_argument("--include-exec-results", action="store_true")
    parser.add_argument("--sql-timeout-sec", type=float, default=3.0)
    args = parser.parse_args()

    data_rows = read_rows(args.data)
    base_rows = read_rows(args.base_all_res)
    n = min(len(data_rows), len(base_rows))
    cand_sets = [load_candidate(spec, n) for spec in args.candidate]
    client = OpenAI(api_key=args.api_key, base_url=args.base_url)

    choices: List[Dict[str, Any]] = [{} for _ in range(n)]
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = {
            ex.submit(
                choose_one,
                idx,
                data_rows[idx],
                cand_sets,
                client,
                args.model,
                args.temperature,
                args.timeout,
                args.db_root if args.include_exec_results else None,
                args.sql_timeout_sec,
            ): idx
            for idx in range(n)
        }
        for fut in as_completed(futures):
            item = fut.result()
            choices[item["idx"]] = item

    merged = []
    for idx in range(n):
        row = dict(base_rows[idx])
        row["pred_sql"] = choices[idx]["pred_sql"]
        row["selected_sql_source"] = choices[idx]["choice"]
        row["selector_raw_response"] = choices[idx]["raw_response"]
        row["selector_error"] = choices[idx]["error"]
        merged.append(row)

    token_usage = {
        "num_cases": n,
        "prompt_tokens": sum(item["token_usage"]["prompt_tokens"] for item in choices),
        "completion_tokens": sum(item["token_usage"]["completion_tokens"] for item in choices),
        "total_tokens": sum(item["token_usage"]["total_tokens"] for item in choices),
        "num_missing_usage": sum(item["token_usage"]["num_missing_usage"] for item in choices),
    }
    latency = {
        "num_cases": n,
        "avg_sec_per_case": sum(item["latency_sec"] for item in choices) / max(n, 1),
        "max_sec_per_case": max((item["latency_sec"] for item in choices), default=0.0),
    }
    counts: Dict[str, int] = {}
    for item in choices:
        counts[item["choice"]] = counts.get(item["choice"], 0) + 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.output_dir / "all_res.deepseek_rerank.json", merged)
    write_json(args.output_dir / "selector_choices.deepseek_rerank.json", choices)
    write_json(args.output_dir / "token_usage.deepseek_rerank.json", token_usage)
    write_json(args.output_dir / "latency.deepseek_rerank.json", latency)
    write_json(args.output_dir / "selection_counts.deepseek_rerank.json", counts)
    print(json.dumps({"num_cases": n, "selection_counts": counts, "token_usage": token_usage, "latency": latency}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
