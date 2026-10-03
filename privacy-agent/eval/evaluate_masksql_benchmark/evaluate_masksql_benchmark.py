#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified evaluator for MaskSQL benchmark.

It can evaluate:
1. MaskSQL official output.json
2. Your privacy-preserving NL2SQL agent output
3. Any method that outputs pred_sql + masked_terms

Metrics:
- Execution Accuracy, EA
- Masking Recall, MR
- Re-identification Score, RI, if attack field exists
- Avg token usage
- Avg latency

Expected input files:
- --gold-file: MaskSQL data/1_input.json
- --pred-file: model output json/jsonl
- --db-dir: MaskSQL data/databases
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional


def load_json_or_jsonl(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if path.suffix == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    data = json.loads(text)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Support {"data": [...]} or {"results": [...]}
        for key in ["data", "results", "items"]:
            if key in data and isinstance(data[key], list):
                return data[key]
    raise ValueError(f"Unsupported file format: {path}")


def save_json(path: str | Path, data: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_idx(row: dict[str, Any], fallback: int) -> str:
    for key in ["idx", "id", "question_id"]:
        if key in row and row[key] is not None:
            return str(row[key])
    return str(fallback)


def build_gold_map(gold_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    gold_map = {}
    for i, row in enumerate(gold_rows):
        idx = normalize_idx(row, i)
        gold_map[idx] = row
    return gold_map


def find_sqlite_db(db_dir: str | Path, db_id: str) -> Path:
    db_dir = Path(db_dir)

    candidates = [
        db_dir / db_id / f"{db_id}.sqlite",
        db_dir / db_id / f"{db_id}.db",
        db_dir / f"{db_id}.sqlite",
        db_dir / f"{db_id}.db",
    ]

    for p in candidates:
        if p.exists():
            return p

    # Fallback: search inside db_id directory
    search_root = db_dir / db_id
    if search_root.exists():
        for ext in ["*.sqlite", "*.db"]:
            found = list(search_root.glob(ext))
            if found:
                return found[0]

    raise FileNotFoundError(f"Cannot find sqlite db for db_id={db_id} under {db_dir}")


def execute_sqlite(
    db_path: str | Path,
    sql: str,
    timeout_s: float = 30.0,
) -> tuple[Optional[list[tuple[Any, ...]]], Optional[str]]:
    """
    Execute SQL with a sqlite progress handler timeout.
    Returns: (rows, error_message)
    """
    if not sql or not isinstance(sql, str):
        return None, "Empty SQL"

    start = time.perf_counter()

    try:
        conn = sqlite3.connect(str(db_path))
        conn.text_factory = lambda b: b.decode(errors="replace")

        def progress_handler() -> int:
            if time.perf_counter() - start > timeout_s:
                return 1
            return 0

        conn.set_progress_handler(progress_handler, 10_000)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        conn.close()
        return rows, None

    except Exception as e:
        return None, str(e)


def normalize_cell(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, float):
        return round(x, 6)
    return x


def normalize_rows(rows: Optional[list[tuple[Any, ...]]]) -> Optional[list[tuple[Any, ...]]]:
    if rows is None:
        return None
    return [tuple(normalize_cell(v) for v in row) for row in rows]


def has_order_by(sql: str) -> bool:
    return bool(re.search(r"\border\s+by\b", sql or "", flags=re.IGNORECASE))


def compare_sql_results(
    gold_rows: Optional[list[tuple[Any, ...]]],
    pred_rows: Optional[list[tuple[Any, ...]]],
    gold_sql: str,
    compare_mode: str = "masksql_exact",
) -> bool:
    """
    compare_mode:
    - masksql_exact: reproduce MaskSQL behavior, direct list equality
    - bird_flexible: if no ORDER BY, sort results before comparing
    """
    gold_norm = normalize_rows(gold_rows)
    pred_norm = normalize_rows(pred_rows)

    if gold_norm is None or pred_norm is None:
        return False

    if compare_mode == "masksql_exact":
        return gold_norm == pred_norm

    if compare_mode == "bird_flexible":
        if not has_order_by(gold_sql):
            try:
                return sorted(gold_norm) == sorted(pred_norm)
            except TypeError:
                return sorted(map(str, gold_norm)) == sorted(map(str, pred_norm))
        return gold_norm == pred_norm

    raise ValueError(f"Unknown compare_mode: {compare_mode}")


def get_nested(row: dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = row
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def extract_pred_sql(row: dict[str, Any]) -> str:
    """
    Support MaskSQL output and your agent output.
    """
    candidates = [
        "pred_sql",
        "prediction",
        "sql",
        "final_sql",
        "concrete_sql",
    ]
    for key in candidates:
        if isinstance(row.get(key), str) and row[key].strip():
            return row[key].strip()

    # Some pipelines may store it in nested fields.
    nested_candidates = [
        "output.pred_sql",
        "result.pred_sql",
        "agent.pred_sql",
    ]
    for path in nested_candidates:
        val = get_nested(row, path)
        if isinstance(val, str) and val.strip():
            return val.strip()

    return ""


def extract_masked_terms(row: dict[str, Any]) -> list[str]:
    """
    MaskSQL official output stores terms in symbolic.masked_terms.
    Your agent can store masked_terms directly.
    """
    direct = row.get("masked_terms")
    if isinstance(direct, list):
        return [str(x) for x in direct if str(x).strip()]

    symbolic_terms = get_nested(row, "symbolic.masked_terms")
    if isinstance(symbolic_terms, list):
        return [str(x) for x in symbolic_terms if str(x).strip()]

    privacy_terms = get_nested(row, "privacy.masked_terms")
    if isinstance(privacy_terms, list):
        return [str(x) for x in privacy_terms if str(x).strip()]

    return []


def extract_masked_question(row: dict[str, Any]) -> str:
    candidates = [
        "masked_question",
        "protected_question",
        "rewritten_question",
    ]
    for key in candidates:
        val = row.get(key)
        if isinstance(val, str):
            return val

    symbolic_q = get_nested(row, "symbolic.question")
    if isinstance(symbolic_q, str):
        return symbolic_q

    return ""


def extract_attack(row: dict[str, Any]) -> str:
    val = row.get("attack")
    if isinstance(val, str):
        return val

    val = get_nested(row, "privacy.attack")
    if isinstance(val, str):
        return val

    return ""


def extract_total_toks(row: dict[str, Any]) -> Optional[float]:
    for key in ["total_toks", "tokens", "token_usage", "total_tokens"]:
        val = row.get(key)
        if isinstance(val, (int, float)):
            return float(val)
    return None


def extract_latency(row: dict[str, Any]) -> Optional[float]:
    for key in ["total_latency", "latency", "time", "elapsed"]:
        val = row.get(key)
        if isinstance(val, (int, float)):
            return float(val)
    return None


def extract_annotated_links(row: dict[str, Any]) -> dict[str, Any]:
    """
    MaskSQL input has annotated_links as ground-truth sensitive token annotations.
    Format is usually:
    {
      "sensitive term": "...schema item..."
    }
    """
    val = row.get("annotated_links", {})
    if isinstance(val, dict):
        return val
    return {}


def contains_term(masked_terms: list[str], gold_term: str) -> bool:
    gold = gold_term.lower().strip()
    if not gold:
        return False

    for term in masked_terms:
        term_l = term.lower().strip()
        # Follow MaskSQL Results logic: ground-truth term covered if it appears
        # inside one of the actually masked terms.
        if gold in term_l or term_l in gold:
            return True
    return False


def masking_recall(
    annotated_links: dict[str, Any],
    masked_terms: list[str],
) -> Optional[float]:
    """
    MR = correctly masked sensitive terms / all ground-truth sensitive terms.

    If a sample has no annotated sensitive terms, return 1.0, same spirit as
    MaskSQL Results where no annotated masks means full score.
    """
    gold_terms = [str(k) for k in annotated_links.keys() if str(k).strip()]
    if len(gold_terms) == 0:
        return 1.0

    hit = 0
    for gold_term in gold_terms:
        if contains_term(masked_terms, gold_term):
            hit += 1

    return hit / len(gold_terms)


def reidentification_score(
    masked_terms: list[str],
    attack_text: str,
) -> Optional[float]:
    """
    RI = proportion of masked terms not recovered by attacker.

    This follows MaskSQL's implementation style:
    if a masked original term appears in attack text, treat it as re-identified.
    """
    if not attack_text:
        return None

    if len(masked_terms) == 0:
        return None

    attack_l = attack_text.lower()
    recovered = 0
    for term in masked_terms:
        if term.lower() in attack_l:
            recovered += 1

    return 1.0 - recovered / len(masked_terms)


def evaluate_one(
    gold: dict[str, Any],
    pred: dict[str, Any],
    db_dir: str | Path,
    timeout_s: float,
    compare_mode: str,
) -> dict[str, Any]:
    idx = str(gold.get("idx", pred.get("idx", "")))
    db_id = str(gold.get("db_id", pred.get("db_id", "")))
    question = str(gold.get("question", pred.get("question", "")))
    gold_sql = str(gold.get("query", gold.get("SQL", ""))).strip()
    pred_sql = extract_pred_sql(pred)

    db_path = find_sqlite_db(db_dir, db_id)

    gold_res, gold_err = execute_sqlite(db_path, gold_sql, timeout_s=timeout_s)
    pred_res, pred_err = execute_sqlite(db_path, pred_sql, timeout_s=timeout_s)

    ea = 1.0 if (
        gold_err is None
        and pred_err is None
        and compare_sql_results(gold_res, pred_res, gold_sql, compare_mode)
    ) else 0.0

    masked_terms = extract_masked_terms(pred)
    annotated_links = extract_annotated_links(gold)
    mr = masking_recall(annotated_links, masked_terms)

    attack = extract_attack(pred)
    ri = reidentification_score(masked_terms, attack)

    return {
        "idx": idx,
        "db_id": db_id,
        "question": question,
        "gold_sql": gold_sql,
        "pred_sql": pred_sql,
        "ea": ea,
        "mr": mr,
        "ri": ri,
        "gold_err": gold_err or "",
        "pred_err": pred_err or "",
        "masked_question": extract_masked_question(pred),
        "masked_terms": masked_terms,
        "attack": attack,
        "total_toks": extract_total_toks(pred),
        "total_latency": extract_latency(pred),
    }


def mean_ignore_none(values: list[Optional[float]]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-file", required=True, help="MaskSQL data/1_input.json")
    parser.add_argument("--pred-file", required=True, help="method output json/jsonl")
    parser.add_argument("--db-dir", required=True, help="MaskSQL data/databases")
    parser.add_argument("--out-file", required=True, help="detailed output json")
    parser.add_argument(
        "--compare-mode",
        default="masksql_exact",
        choices=["masksql_exact", "bird_flexible"],
        help="masksql_exact reproduces MaskSQL CalcExecAcc; bird_flexible sorts unordered results.",
    )
    parser.add_argument("--timeout-s", type=float, default=30.0)
    args = parser.parse_args()

    gold_rows = load_json_or_jsonl(args.gold_file)
    pred_rows = load_json_or_jsonl(args.pred_file)

    gold_map = build_gold_map(gold_rows)

    details = []
    missing = 0

    for i, pred in enumerate(pred_rows):
        idx = normalize_idx(pred, i)
        if idx not in gold_map:
            missing += 1
            continue

        row_result = evaluate_one(
            gold=gold_map[idx],
            pred=pred,
            db_dir=args.db_dir,
            timeout_s=args.timeout_s,
            compare_mode=args.compare_mode,
        )
        details.append(row_result)

    summary = {
        "num_gold": len(gold_rows),
        "num_pred": len(pred_rows),
        "num_eval": len(details),
        "num_missing_pred_idx_in_gold": missing,
        "execution_accuracy": mean_ignore_none([x["ea"] for x in details]),
        "masking_recall": mean_ignore_none([x["mr"] for x in details]),
        "reidentification_score": mean_ignore_none([x["ri"] for x in details]),
        "avg_total_toks": mean_ignore_none([x["total_toks"] for x in details]),
        "avg_total_latency": mean_ignore_none([x["total_latency"] for x in details]),
    }

    save_json(args.out_file, {"summary": summary, "details": details})

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()