#!/usr/bin/env python3
"""Train a non-oracle SQL candidate selector on Spider-dev.

The selector uses dev execution-match labels to learn which candidate is likely
correct from candidate-local signals. At test time it never sees gold SQL or
execution labels; it selects the candidate with the highest predicted
probability of being correct.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import Pipeline


SQL_KEYWORDS = [
    "select",
    "where",
    "join",
    "group by",
    "order by",
    "limit",
    "distinct",
    "having",
    "union",
    "intersect",
    "except",
    "count(",
    "avg(",
    "sum(",
    "min(",
    "max(",
]


@dataclass
class CandidateSpec:
    name: str
    path: Path


def read_rows(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    data = json.loads(text)
    if isinstance(data, dict):
        for key in ("data", "examples", "items"):
            if isinstance(data.get(key), list):
                return data[key]
    if not isinstance(data, list):
        raise ValueError(f"Unsupported JSON rows: {path}")
    return data


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_specs(values: List[str]) -> List[CandidateSpec]:
    specs = []
    for value in values:
        name, path = value.split("=", 1)
        specs.append(CandidateSpec(name=name, path=Path(path)))
    return specs


def load_sqls(spec: CandidateSpec, n: int) -> List[str]:
    if spec.path.suffix == ".sql":
        sqls = spec.path.read_text(encoding="utf-8").splitlines()
    else:
        rows = read_rows(spec.path)
        sqls = [str(row.get("pred_sql") or row.get("predict_sql") or row.get("selected_sql_candidate") or "") for row in rows]
    if len(sqls) < n:
        sqls.extend([""] * (n - len(sqls)))
    return sqls[:n]


def load_labels(path: Path) -> List[int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    labels = [0] * int(data["num_cases"])
    for item in data["per_case"]:
        labels[int(item["idx"])] = int(bool(item.get("match")))
    return labels


def normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def run_sql_features(db_path: Path, sql: str, timeout_sec: float, max_rows: int = 8) -> Dict[str, Any]:
    if not sql.strip():
        return {"exec_ok": 0, "exec_error_type": "empty", "row_count": 0, "col_count": 0, "first_nulls": 0}
    try:
        conn = sqlite3.connect(str(db_path))
        conn.text_factory = lambda raw: raw.decode("utf-8", errors="replace")
        started = time.monotonic()

        def stop_slow_query() -> int:
            return 1 if time.monotonic() - started > timeout_sec else 0

        conn.set_progress_handler(stop_slow_query, 10000)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchmany(max_rows)
        conn.close()
        normalized = [[normalize_value(v) for v in row] for row in rows]
        row_count = len(normalized)
        col_count = len(normalized[0]) if normalized else 0
        first_nulls = sum(1 for row in normalized[:8] for v in row if v is None)
        numeric_cells = sum(1 for row in normalized[:8] for v in row if isinstance(v, (int, float)))
        text_cells = sum(1 for row in normalized[:8] for v in row if isinstance(v, str))
        result_json = json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str)
        result_hash = hashlib.sha1(result_json.encode("utf-8", errors="replace")).hexdigest()
        return {
            "exec_ok": 1,
            "exec_error_type": "none",
            "result_hash": result_hash,
            "row_count": row_count,
            "row_count_ge_64": int(row_count >= max_rows),
            "col_count": col_count,
            "first_nulls": first_nulls,
            "numeric_cells": numeric_cells,
            "text_cells": text_cells,
        }
    except Exception as exc:
        return {
            "exec_ok": 0,
            "exec_error_type": type(exc).__name__,
            "result_hash": f"ERR:{type(exc).__name__}",
            "row_count": 0,
            "row_count_ge_64": 0,
            "col_count": 0,
            "first_nulls": 0,
            "numeric_cells": 0,
            "text_cells": 0,
        }


def base_features(row: Dict[str, Any], cand_name: str, sql: str, db_root: Path, timeout_sec: float, include_source: bool) -> Dict[str, Any]:
    q = str(row.get("input") or row.get("question") or "").lower()
    sql_l = sql.lower()
    feats: Dict[str, Any] = {}
    if include_source:
        feats["source"] = cand_name
    feats["sql_len"] = len(sql)
    feats["sql_tokens"] = len(re.findall(r"\w+", sql_l))
    feats["question_tokens"] = len(re.findall(r"\w+", q))
    feats["has_semicolon"] = int(";" in sql)
    feats["has_backtick"] = int("`" in sql)
    feats["has_bracket_quote"] = int("[" in sql or "]" in sql)
    feats["has_star"] = int("*" in sql)
    feats["paren_balance"] = sql.count("(") - sql.count(")")
    for kw in SQL_KEYWORDS:
        feats[f"sql_kw:{kw}"] = int(kw in sql_l)
        feats[f"q_kw:{kw}"] = int(kw.replace("(", "") in q)
    feats["select_count"] = sql_l.count("select")
    feats["join_count"] = sql_l.count(" join ")
    feats["predicate_count"] = sql_l.count(" and ") + sql_l.count(" or ")
    feats["mentions_count_when_q_count"] = int("count" in sql_l and any(w in q for w in ["how many", "number of", "count"]))
    feats["mentions_order_when_q_order"] = int("order by" in sql_l and any(w in q for w in ["order", "sort", "top", "most", "least", "highest", "lowest"]))
    feats["mentions_group_when_q_each"] = int("group by" in sql_l and any(w in q for w in ["each", "per ", "for each", "by "]))
    db_id = str(row.get("db_id", ""))
    feats.update(run_sql_features(db_root / db_id / f"{db_id}.sqlite", sql, timeout_sec))
    return feats


def add_consensus_features(case_items: List[Dict[str, Any]]) -> None:
    hash_counts: Dict[str, int] = {}
    ok_count = 0
    for item in case_items:
        result_hash = str(item.get("result_hash", ""))
        hash_counts[result_hash] = hash_counts.get(result_hash, 0) + 1
        ok_count += int(item.get("exec_ok", 0))
    max_count = max(hash_counts.values(), default=0)
    n = max(len(case_items), 1)
    for item in case_items:
        result_hash = str(item.get("result_hash", ""))
        count = hash_counts.get(result_hash, 0)
        item["consensus_count"] = count
        item["consensus_frac"] = count / n
        item["is_consensus_mode"] = int(count == max_count and count > 1)
        item["exec_ok_count_in_case"] = ok_count
        item["exec_ok_frac_in_case"] = ok_count / n
        item["unique_result_count_in_case"] = len(hash_counts)
        # Avoid leaking a case-specific high-cardinality hash into the model.
        item.pop("result_hash", None)


def static_features(row: Dict[str, Any], cand_name: str, sql: str, include_source: bool) -> Dict[str, Any]:
    q = str(row.get("input") or row.get("question") or "").lower()
    sql_l = sql.lower()
    feats: Dict[str, Any] = {}
    if include_source:
        feats["source"] = cand_name
    feats["sql_len"] = len(sql)
    feats["sql_tokens"] = len(re.findall(r"\w+", sql_l))
    feats["question_tokens"] = len(re.findall(r"\w+", q))
    feats["has_semicolon"] = int(";" in sql)
    feats["has_backtick"] = int("`" in sql)
    feats["has_bracket_quote"] = int("[" in sql or "]" in sql)
    feats["has_star"] = int("*" in sql)
    feats["paren_balance"] = sql.count("(") - sql.count(")")
    for kw in SQL_KEYWORDS:
        feats[f"sql_kw:{kw}"] = int(kw in sql_l)
        feats[f"q_kw:{kw}"] = int(kw.replace("(", "") in q)
    feats["select_count"] = sql_l.count("select")
    feats["join_count"] = sql_l.count(" join ")
    feats["predicate_count"] = sql_l.count(" and ") + sql_l.count(" or ")
    feats["mentions_count_when_q_count"] = int("count" in sql_l and any(w in q for w in ["how many", "number of", "count"]))
    feats["mentions_order_when_q_order"] = int("order by" in sql_l and any(w in q for w in ["order", "sort", "top", "most", "least", "highest", "lowest"]))
    feats["mentions_group_when_q_each"] = int("group by" in sql_l and any(w in q for w in ["each", "per ", "for each", "by "]))
    return feats


def make_examples(
    rows: List[Dict[str, Any]],
    specs: List[CandidateSpec],
    labels_by_name: Dict[str, List[int]] | None,
    db_root: Path,
    timeout_sec: float,
    include_source: bool,
    skip_exec_features: bool = False,
    consensus_features: bool = False,
) -> Tuple[List[Dict[str, Any]], List[int] | None, List[Tuple[int, str, str]]]:
    n = len(rows)
    sqls_by_name = {spec.name: load_sqls(spec, n) for spec in specs}
    feats: List[Dict[str, Any]] = []
    labels: List[int] = []
    meta: List[Tuple[int, str, str]] = []
    for idx, row in enumerate(rows):
        case_feats: List[Dict[str, Any]] = []
        case_labels: List[int] = []
        case_meta: List[Tuple[int, str, str]] = []
        for spec in specs:
            sql = sqls_by_name[spec.name][idx]
            if skip_exec_features:
                item_feats = static_features(row, spec.name, sql, include_source)
            else:
                item_feats = base_features(row, spec.name, sql, db_root, timeout_sec, include_source)
            case_feats.append(item_feats)
            if labels_by_name is not None:
                case_labels.append(labels_by_name[spec.name][idx])
            case_meta.append((idx, spec.name, sql))
        if consensus_features and not skip_exec_features:
            add_consensus_features(case_feats)
        feats.extend(case_feats)
        labels.extend(case_labels)
        meta.extend(case_meta)
    return feats, labels if labels_by_name is not None else None, meta


def select_rows(
    probs: np.ndarray,
    meta: List[Tuple[int, str, str]],
    base_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    best: Dict[int, Tuple[float, str, str]] = {}
    for prob, (idx, name, sql) in zip(probs, meta):
        if idx not in best or prob > best[idx][0]:
            best[idx] = (float(prob), name, sql)
    out = []
    counts: Dict[str, int] = {}
    for idx, row in enumerate(base_rows):
        prob, name, sql = best[idx]
        new_row = dict(row)
        new_row["pred_sql"] = sql
        new_row["selected_sql_source"] = name
        new_row["selected_sql_score"] = prob
        out.append(new_row)
        counts[name] = counts.get(name, 0) + 1
    return out, counts


def select_from_scores(
    scored: List[Dict[str, Any]],
    base_rows: List[Dict[str, Any]],
    source_bias: Dict[str, float] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    source_bias = source_bias or {}
    best: Dict[int, Dict[str, Any]] = {}
    for item in scored:
        score = float(item["prob_correct"]) + float(source_bias.get(item["source"], 0.0))
        idx = int(item["idx"])
        if idx not in best or score > best[idx]["biased_score"]:
            best[idx] = {**item, "biased_score": score}
    out = []
    counts: Dict[str, int] = {}
    for idx, row in enumerate(base_rows):
        item = best[idx]
        new_row = dict(row)
        new_row["pred_sql"] = item["sql"]
        new_row["selected_sql_source"] = item["source"]
        new_row["selected_sql_score"] = item["prob_correct"]
        new_row["selected_sql_biased_score"] = item["biased_score"]
        out.append(new_row)
        counts[item["source"]] = counts.get(item["source"], 0) + 1
    return out, counts


def optimize_source_bias(
    oof_scores: List[Dict[str, Any]],
    labels_by_idx_source: Dict[Tuple[int, str], int],
    sources: List[str],
) -> Tuple[Dict[str, float], float, Dict[str, int]]:
    best_bias = {source: 0.0 for source in sources}
    best_acc = -1.0
    best_counts: Dict[str, int] = {}
    # Keep the grid deliberately small to reduce overfitting; these are
    # probability offsets learned only from Spider-dev OOF scores.
    grid = [-0.12, -0.08, -0.04, 0.0, 0.04, 0.08, 0.12]
    locked = sources[0] if sources else ""

    def eval_bias(bias: Dict[str, float]) -> Tuple[float, Dict[str, int]]:
        best: Dict[int, Dict[str, Any]] = {}
        for item in oof_scores:
            score = float(item["prob_correct"]) + bias.get(item["source"], 0.0)
            idx = int(item["idx"])
            if idx not in best or score > best[idx]["score"]:
                best[idx] = {"source": item["source"], "score": score}
        correct = 0
        counts: Dict[str, int] = {}
        for idx, item in best.items():
            source = item["source"]
            correct += int(labels_by_idx_source.get((idx, source), 0))
            counts[source] = counts.get(source, 0) + 1
        return correct / max(len(best), 1), counts

    current = {source: 0.0 for source in sources}
    best_acc, best_counts = eval_bias(current)
    best_bias = dict(current)
    # Coordinate search keeps the calibration dev-only while avoiding the
    # exponential cost of full grid search for larger candidate pools.
    improved = True
    while improved:
        improved = False
        for source in sources:
            if source == locked:
                continue
            local_best = (best_acc, current[source], best_counts)
            for value in grid:
                trial = dict(current)
                trial[source] = value
                acc, counts = eval_bias(trial)
                if acc > local_best[0]:
                    local_best = (acc, value, counts)
            if local_best[0] > best_acc:
                current[source] = local_best[1]
                best_acc = local_best[0]
                best_counts = local_best[2]
                best_bias = dict(current)
                improved = True
    return best_bias, best_acc, best_counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dev-data", type=Path, required=True)
    parser.add_argument("--test-data", type=Path, required=True)
    parser.add_argument("--base-test-all-res", type=Path, required=True)
    parser.add_argument("--dev-candidate", action="append", required=True)
    parser.add_argument("--test-candidate", action="append", required=True)
    parser.add_argument("--dev-label", action="append", required=True, help="name=/path/to/exec_eval_simple.json")
    parser.add_argument("--dev-db-root", type=Path, required=True)
    parser.add_argument("--test-db-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout-sec", type=float, default=3.0)
    parser.add_argument("--model", choices=["logreg", "rf"], default="rf")
    parser.add_argument("--no-source-feature", action="store_true")
    parser.add_argument("--skip-exec-features", action="store_true")
    parser.add_argument("--consensus-features", action="store_true")
    args = parser.parse_args()

    include_source = not args.no_source_feature
    dev_rows = read_rows(args.dev_data)
    test_rows = read_rows(args.test_data)
    base_test_rows = read_rows(args.base_test_all_res)
    dev_specs = parse_specs(args.dev_candidate)
    test_specs = parse_specs(args.test_candidate)
    label_paths = dict(item.split("=", 1) for item in args.dev_label)
    labels_by_name = {name: load_labels(Path(path)) for name, path in label_paths.items()}

    x_dev, y_dev, meta_dev = make_examples(dev_rows, dev_specs, labels_by_name, args.dev_db_root, args.timeout_sec, include_source, args.skip_exec_features, args.consensus_features)
    x_test, _, meta_test = make_examples(test_rows, test_specs, None, args.test_db_root, args.timeout_sec, include_source, args.skip_exec_features, args.consensus_features)

    if args.model == "rf":
        clf = RandomForestClassifier(n_estimators=400, min_samples_leaf=4, random_state=13, class_weight="balanced_subsample", n_jobs=-1)
    else:
        clf = LogisticRegression(max_iter=2000, class_weight="balanced")
    pipe = Pipeline([("vec", DictVectorizer(sparse=False)), ("clf", clf)])

    groups = np.array([idx for idx, _, _ in meta_dev])
    y_arr = np.array(y_dev)
    cv_rows = []
    oof_scores: List[Dict[str, Any]] = []
    for fold, (tr, va) in enumerate(GroupKFold(n_splits=5).split(x_dev, y_arr, groups)):
        fold_pipe = Pipeline([("vec", DictVectorizer(sparse=False)), ("clf", clf.__class__(**clf.get_params()))])
        fold_pipe.fit([x_dev[i] for i in tr], y_arr[tr])
        prob = fold_pipe.predict_proba([x_dev[i] for i in va])[:, 1]
        pred = (prob >= 0.5).astype(int)
        try:
            auc = roc_auc_score(y_arr[va], prob)
        except Exception:
            auc = None
        cv_rows.append({"fold": fold, "binary_accuracy": float(accuracy_score(y_arr[va], pred)), "binary_auc": auc})
        for local_i, global_i in enumerate(va):
            idx, source, sql = meta_dev[global_i]
            oof_scores.append(
                {
                    "idx": idx,
                    "source": source,
                    "sql": sql,
                    "prob_correct": float(prob[local_i]),
                    "label": int(y_arr[global_i]),
                    "fold": fold,
                }
            )

    pipe.fit(x_dev, y_arr)
    test_probs = pipe.predict_proba(x_test)[:, 1]
    test_scores = [
        {"idx": idx, "source": source, "sql": sql, "prob_correct": float(prob)}
        for prob, (idx, source, sql) in zip(test_probs, meta_test)
    ]
    labels_by_idx_source = {(idx, source): int(label) for (idx, source, _), label in zip(meta_dev, y_dev)}
    sources = [spec.name for spec in test_specs]
    source_bias, oof_select_acc, oof_select_counts = optimize_source_bias(oof_scores, labels_by_idx_source, sources)
    selected, counts = select_from_scores(test_scores, base_test_rows)
    selected_biased, counts_biased = select_from_scores(test_scores, base_test_rows, source_bias)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.output_dir / "all_res.dev_supervised_selector.json", selected)
    write_json(args.output_dir / "all_res.dev_supervised_selector.biased.json", selected_biased)
    write_json(args.output_dir / "selection_counts.dev_supervised_selector.json", counts)
    write_json(args.output_dir / "selection_counts.dev_supervised_selector.biased.json", counts_biased)
    write_json(args.output_dir / "cv.dev_supervised_selector.json", cv_rows)
    write_json(args.output_dir / "oof_scores.dev_supervised_selector.json", oof_scores)
    write_json(args.output_dir / "test_scores.dev_supervised_selector.json", test_scores)
    summary = {
        "num_dev_examples": len(dev_rows),
        "num_test_examples": len(test_rows),
        "num_dev_candidate_examples": len(x_dev),
        "num_test_candidate_examples": len(x_test),
        "model": args.model,
        "include_source_feature": include_source,
        "skip_exec_features": args.skip_exec_features,
        "consensus_features": args.consensus_features,
        "selection_counts": counts,
        "selection_counts_biased": counts_biased,
        "source_bias": source_bias,
        "oof_select_accuracy_biased": oof_select_acc,
        "oof_select_counts_biased": oof_select_counts,
        "cv": cv_rows,
    }
    write_json(args.output_dir / "summary.dev_supervised_selector.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
