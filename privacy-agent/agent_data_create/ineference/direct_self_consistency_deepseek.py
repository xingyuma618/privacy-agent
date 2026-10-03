#!/usr/bin/env python3
"""Direct Text-to-SQL self-consistency baseline.

This is a non-oracle direct prompting variant: for each example, call the cloud
model multiple times, execute the candidate SQLs, and select the valid candidate
whose execution result agrees with the largest number of candidates. It never
uses gold SQL or correctness labels for selection.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

from openai import OpenAI


def read_rows(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return json.loads(text)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


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
    if "```sql" in text.lower():
        matches = re.findall(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
        if matches:
            return clean_space(matches[-1].replace("\n", " ").strip())
    if "```" in text:
        matches = re.findall(r"```\s*(.*?)```", text, re.DOTALL)
        if matches:
            return clean_space(matches[-1].replace("\n", " ").strip())
    text = re.sub(r"^\s*(SQL|Answer|Final SQL)\s*:\s*", "", text.strip(), flags=re.IGNORECASE)
    return clean_space(text.replace("\n", " ").strip())


def build_messages(row: Dict[str, Any]) -> List[Dict[str, str]]:
    schema = row.get("db_schema", "")
    question = row.get("question", "")
    evidence = row.get("evidence", "")
    return [
        {
            "role": "system",
            "content": (
                "You are an expert Spider Text-to-SQL solver for SQLite. "
                "Return exactly one executable SQL query and no explanation."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Database schema:\n{schema}\n\n"
                f"Question:\n{question}\n\n"
                f"Evidence:\n{evidence}\n\n"
                "Write the final SQLite SQL query only."
            ),
        },
    ]


def usage_to_dict(response: Any) -> Dict[str, int]:
    usage = getattr(response, "usage", None)
    if usage is None:
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "num_missing_usage": 1}
    prompt = int(getattr(usage, "prompt_tokens", 0) or 0)
    completion = int(getattr(usage, "completion_tokens", 0) or 0)
    total = int(getattr(usage, "total_tokens", 0) or prompt + completion)
    return {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total, "num_missing_usage": 0}


def normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def run_sql_hash(db_path: Path, sql: str, timeout_sec: float) -> Dict[str, Any]:
    if not sql.strip():
        return {"ok": False, "hash": "ERR:empty", "error": "empty sql"}
    try:
        conn = sqlite3.connect(str(db_path))
        conn.text_factory = lambda raw: raw.decode("utf-8", errors="replace")
        started = time.monotonic()

        def stop_slow_query() -> int:
            return 1 if time.monotonic() - started > timeout_sec else 0

        conn.set_progress_handler(stop_slow_query, 10000)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        conn.close()
        normalized = [tuple(normalize_value(v) for v in row) for row in rows]
        try:
            normalized = sorted(normalized)
        except TypeError:
            normalized = sorted(map(str, normalized))
        blob = json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str)
        return {"ok": True, "hash": hashlib.sha1(blob.encode("utf-8", errors="replace")).hexdigest(), "error": ""}
    except Exception as exc:
        return {"ok": False, "hash": f"ERR:{type(exc).__name__}", "error": f"{type(exc).__name__}: {exc}"}


def call_one(client: OpenAI, model: str, row: Dict[str, Any], temperature: float, timeout: float) -> Tuple[str, Dict[str, int], str]:
    try:
        response = client.chat.completions.create(
            model=model,
            messages=build_messages(row),
            temperature=temperature,
            timeout=timeout,
        )
        text = response.choices[0].message.content or ""
        return find_result(text), usage_to_dict(response), ""
    except Exception as exc:
        return "", {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "num_missing_usage": 1}, f"{type(exc).__name__}: {exc}"


def process_row(idx: int, row: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    client = OpenAI(api_key=args.api_key, base_url=args.base_url, timeout=args.request_timeout)
    db_id = str(row.get("db_id", ""))
    db_path = args.db_root / db_id / f"{db_id}.sqlite"
    candidates = []
    for sample_idx in range(args.num_samples):
        sql, usage, error = call_one(client, args.model_name, row, args.temperature, args.request_timeout)
        exec_info = run_sql_hash(db_path, sql, args.sql_timeout_sec)
        candidates.append({"sample_idx": sample_idx, "sql": sql, "usage": usage, "error": error, "exec": exec_info})

    hash_counts: Dict[str, int] = {}
    for cand in candidates:
        if cand["exec"]["ok"]:
            hash_counts[cand["exec"]["hash"]] = hash_counts.get(cand["exec"]["hash"], 0) + 1
    best = None
    for cand in candidates:
        ok = cand["exec"]["ok"]
        count = hash_counts.get(cand["exec"]["hash"], 0) if ok else 0
        key = (int(ok), count, -cand["sample_idx"])
        if best is None or key > best[0]:
            best = (key, cand)
    selected = best[1] if best else candidates[0]
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "num_missing_usage": 0}
    for cand in candidates:
        for key in total_usage:
            total_usage[key] += int(cand["usage"].get(key, 0) or 0)
    return {
        "eval_original_idx": row.get("question_id", idx),
        "question_id": row.get("question_id", idx),
        "case_id": row.get("question_id", idx),
        "db_id": db_id,
        "input": row.get("question", ""),
        "question": row.get("question", ""),
        "evidence": row.get("evidence", ""),
        "db_schema": row.get("db_schema", ""),
        "gold_sql": row.get("gold_sql", ""),
        "pred_sql": selected["sql"],
        "selected_sample_idx": selected["sample_idx"],
        "candidate_sqls": candidates,
        "token_usage": total_usage,
        "total_tokens": total_usage["total_tokens"],
        "prompt_tokens": total_usage["prompt_tokens"],
        "completion_tokens": total_usage["completion_tokens"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--base-url", default="https://api.deepseek.com/v1")
    parser.add_argument("--model-name", default="deepseek-chat")
    parser.add_argument("--api-key", default=os.getenv("DEEPSEEK_API_KEY", ""))
    parser.add_argument("--num-samples", type=int, default=3)
    parser.add_argument("--temperature", type=float, default=0.7)
    parser.add_argument("--max-workers", type=int, default=20)
    parser.add_argument("--request-timeout", type=float, default=120)
    parser.add_argument("--sql-timeout-sec", type=float, default=3)
    args = parser.parse_args()

    rows = read_rows(args.data)
    out_rows: List[Dict[str, Any]] = [{} for _ in rows]
    started = time.time()
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = {ex.submit(process_row, idx, row, args): idx for idx, row in enumerate(rows)}
        for fut in as_completed(futures):
            idx = futures[fut]
            out_rows[idx] = fut.result()
            if (idx + 1) % 50 == 0:
                print(f"completed index {idx + 1}/{len(rows)}", flush=True)
    token_usage = {
        "num_cases": len(out_rows),
        "num_samples": args.num_samples,
        "prompt_tokens": sum(int(r["prompt_tokens"]) for r in out_rows),
        "completion_tokens": sum(int(r["completion_tokens"]) for r in out_rows),
        "total_tokens": sum(int(r["total_tokens"]) for r in out_rows),
    }
    latency = {"num_cases": len(out_rows), "wall_sec": time.time() - started, "max_workers": args.max_workers}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.output_dir / "all_res.json", out_rows)
    write_json(args.output_dir / "token_usage.json", token_usage)
    write_json(args.output_dir / "latency.json", latency)
    (args.output_dir / "predict.sql").write_text("\n".join(r["pred_sql"] for r in out_rows) + "\n", encoding="utf-8")
    print(json.dumps({"token_usage": token_usage, "latency": latency}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
