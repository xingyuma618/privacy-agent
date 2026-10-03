#!/usr/bin/env python3
"""Parallel lightweight Spider execution scorer.

This mirrors spider_privacy_generalization.py exec-score, but evaluates cases
concurrently so one slow SQL query does not make full Spider-test scoring drag.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple


def read_rows(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return json.loads(text)


def parse_gold_line(line: str) -> Tuple[str, str]:
    if "\t" in line:
        sql, db_id = line.rsplit("\t", 1)
        return sql.strip(), db_id.strip()
    return line.strip(), ""


def read_pairs_from_sql(gold_path: Path, pred_path: Path) -> List[Dict[str, Any]]:
    gold_lines = [line for line in gold_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    pred_lines = pred_path.read_text(encoding="utf-8").splitlines()
    rows: List[Dict[str, Any]] = []
    for idx in range(min(len(gold_lines), len(pred_lines))):
        gold_sql, db_id = parse_gold_line(gold_lines[idx])
        rows.append({"gold_sql": gold_sql, "db_id": db_id, "pred_sql": pred_lines[idx].strip()})
    return rows


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_exec_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def run_sql(db_path: Path, sql: str, timeout_sec: float) -> Tuple[bool, Any]:
    if not sql.strip():
        return False, "empty sql"
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
        normalized = [tuple(normalize_exec_value(v) for v in row) for row in rows]
        try:
            return True, sorted(normalized)
        except TypeError:
            return True, sorted(map(str, normalized))
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def score_one(idx: int, row: Dict[str, Any], db_root: Path, timeout_sec: float) -> Dict[str, Any]:
    db_id = str(row.get("db_id", ""))
    gold_sql = row.get("gold_sql") or row.get("query", "")
    pred_sql = row.get("pred_sql") or row.get("predict_sql") or ""
    db_path = db_root / db_id / f"{db_id}.sqlite"
    gold_ok, gold_res = run_sql(db_path, gold_sql, timeout_sec)
    pred_ok, pred_res = run_sql(db_path, pred_sql, timeout_sec)
    match = bool(gold_ok and pred_ok and gold_res == pred_res)
    return {
        "idx": idx,
        "db_id": db_id,
        "gold_ok": gold_ok,
        "pred_ok": pred_ok,
        "match": match,
        "gold_error": "" if gold_ok else gold_res,
        "pred_error": "" if pred_ok else pred_res,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--all-res", type=Path, default=None)
    parser.add_argument("--gold-path", type=Path, default=None)
    parser.add_argument("--pred-path", type=Path, default=None)
    parser.add_argument("--db-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--timeout-sec", type=float, default=5.0)
    args = parser.parse_args()

    if args.all_res:
        rows = read_rows(args.all_res)
        source = str(args.all_res)
    elif args.gold_path and args.pred_path:
        rows = read_pairs_from_sql(args.gold_path, args.pred_path)
        source = str(args.pred_path)
    else:
        parser.error("Provide either --all-res or both --gold-path and --pred-path.")
    per_case: List[Dict[str, Any]] = [{} for _ in rows]
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(score_one, idx, row, args.db_root, args.timeout_sec): idx
            for idx, row in enumerate(rows)
        }
        for fut in as_completed(futures):
            item = fut.result()
            per_case[item["idx"]] = item

    correct = sum(1 for item in per_case if item.get("match"))
    valid_pred = sum(1 for item in per_case if item.get("pred_ok"))
    report = {
        "num_cases": len(rows),
        "source": source,
        "exec_accuracy": correct / max(len(rows), 1),
        "valid_pred_rate": valid_pred / max(len(rows), 1),
        "correct": correct,
        "valid_pred": valid_pred,
        "per_case": per_case,
    }
    write_json(args.output, report)
    print(json.dumps({k: report[k] for k in ["num_cases", "exec_accuracy", "valid_pred_rate", "correct", "valid_pred"]}, indent=2))


if __name__ == "__main__":
    main()
