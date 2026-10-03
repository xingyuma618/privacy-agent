#!/usr/bin/env python3
"""Utilities for Spider privacy-generalization experiments.

This script intentionally avoids importing agent_inference_with_privacy_eval_v4.py
because that module loads a tokenizer at import time.  It provides the same
MaskSQL-style annotation matching and privacy scoring needed for a reviewer-facing
Spider privacy experiment.
"""

# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))


import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
import sqlite3
import subprocess
import time
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Tuple

try:
    from openai import OpenAI
    import httpx
except Exception:  # pragma: no cover - allows audit/score without OpenAI installed
    OpenAI = None
    httpx = None

try:
    import json_repair
except Exception:  # pragma: no cover
    json_repair = None


def read_rows(path: Path) -> List[Dict[str, Any]]:
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    path.write_text(text + ("\n" if text else ""), encoding="utf-8")


def norm_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\u00a0", " ").strip())


def norm_sql(sql: Any) -> str:
    text = norm_text(sql).rstrip(";")
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\s*,\s*", ",", text)
    return re.sub(r"\s+", " ", text).lower()


def data_key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        norm_text(row.get("db_id")),
        norm_text(row.get("question") or row.get("input")),
        norm_text(row.get("evidence")),
        norm_sql(row.get("gold_sql")),
    )


def annotation_key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        norm_text(row.get("orig_db_id")),
        norm_text(row.get("orig_question")),
        norm_text(row.get("orig_evidence")),
        norm_sql(row.get("orig_gold_sql")),
    )


def combine_gold_links(row: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    combined: Dict[str, Dict[str, str]] = {}
    for term, target in (row.get("orig_gold_schema_links") or {}).items():
        combined[str(term)] = {"type": "schema", "target": str(target)}
    for term, target in (row.get("orig_gold_value_links") or {}).items():
        term = str(term)
        if term in combined:
            combined[term] = {
                "type": combined[term]["type"] + "+value",
                "target": combined[term]["target"] + " || " + str(target),
            }
        else:
            combined[term] = {"type": "value", "target": str(target)}
    return combined


def attach_annotations(
    data_rows: List[Dict[str, Any]],
    annotation_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    ann_index = {annotation_key(row): row for row in annotation_rows}
    selected: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []
    used = set()

    for row in data_rows:
        ann = ann_index.get(data_key(row))
        if not ann:
            unmatched.append(
                {
                    "db_id": row.get("db_id"),
                    "question_id": row.get("question_id"),
                    "question": row.get("question"),
                    "gold_sql": row.get("gold_sql"),
                }
            )
            continue
        used.add(ann.get("idx"))
        new_row = dict(row)
        new_row["masksql_idx"] = ann.get("idx")
        new_row["masksql_match_method"] = "strict"
        new_row["masksql_orig_question"] = ann.get("orig_question", "")
        new_row["masksql_orig_evidence"] = ann.get("orig_evidence", "")
        new_row["masksql_orig_gold_sql"] = ann.get("orig_gold_sql", "")
        new_row["orig_gold_schema_links"] = ann.get("orig_gold_schema_links", {}) or {}
        new_row["orig_gold_value_links"] = ann.get("orig_gold_value_links", {}) or {}
        new_row["annotated_links"] = combine_gold_links(ann)
        selected.append(new_row)

    report = {
        "data_rows": len(data_rows),
        "annotation_rows": len(annotation_rows),
        "selected_rows": len(selected),
        "matched_annotation_rows": len(used),
        "unmatched_rows": len(unmatched),
        "unmatched_examples": unmatched[:20],
    }
    return selected, report


def audit_annotations(annotation_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    term_counts = []
    schema_count = 0
    value_count = 0
    placeholder_rows = 0
    duplicate_surface_rows = 0
    zero_term_rows = []

    for row in annotation_rows:
        schema_links = row.get("orig_gold_schema_links") or {}
        value_links = row.get("orig_gold_value_links") or {}
        terms = list(schema_links) + list(value_links)
        schema_count += len(schema_links)
        value_count += len(value_links)
        term_counts.append(len(terms))
        if row.get("db_id") == "<DB_ID>" or row.get("question") == "<QUESTION>":
            placeholder_rows += 1
        if len(set(map(str.lower, terms))) != len(terms):
            duplicate_surface_rows += 1
        if not terms:
            zero_term_rows.append(row.get("idx"))

    distribution: Dict[str, int] = {}
    for count in term_counts:
        distribution[str(count)] = distribution.get(str(count), 0) + 1

    return {
        "num_rows": len(annotation_rows),
        "schema_link_count": schema_count,
        "value_link_count": value_count,
        "avg_sensitive_terms_per_row": mean(term_counts) if term_counts else 0.0,
        "placeholder_outer_field_rows": placeholder_rows,
        "duplicate_surface_rows": duplicate_surface_rows,
        "zero_sensitive_term_rows": len(zero_term_rows),
        "zero_sensitive_term_examples": zero_term_rows[:20],
        "term_count_distribution": dict(sorted(distribution.items(), key=lambda kv: int(kv[0]))),
    }


def norm_surface(value: Any) -> str:
    text = norm_text(value).strip("'\"` ").lower()
    return re.sub(r"\s+", " ", text)


def surface_in_text(term: str, text: str) -> bool:
    term_norm = norm_surface(term)
    text_norm = norm_surface(text)
    if not term_norm:
        return False
    if re.fullmatch(r"[a-z0-9_]+", term_norm):
        return re.search(r"(?<![a-z0-9_])" + re.escape(term_norm) + r"(?![a-z0-9_])", text_norm) is not None
    return term_norm in text_norm


def term_covered(term: str, masked_terms: List[str]) -> bool:
    term_norm = norm_surface(term)
    if not term_norm:
        return False
    for masked_term in masked_terms or []:
        mt = norm_surface(masked_term)
        if mt and (term_norm == mt or term_norm in mt or mt in term_norm):
            return True
    return False


def score_case(row: Dict[str, Any]) -> Dict[str, Any]:
    annotated_links = row.get("annotated_links") or {}
    annotated_terms = list(annotated_links.keys())
    masked_terms = row.get("masked_terms") or []
    masked_question = row.get("masked_question") or ""
    masked_schema = row.get("masked_schema") or ""
    masked_prompt = f"{masked_question}\n{masked_schema}"
    original_prompt = f"{row.get('input') or row.get('question', '')} {row.get('evidence', '')} {row.get('final_question', '')}"

    covered_terms = [term for term in annotated_terms if term_covered(term, masked_terms)]
    present_terms = [term for term in annotated_terms if surface_in_text(term, original_prompt)]
    covered_present_terms = [term for term in present_terms if term_covered(term, masked_terms)]
    direct_leak_terms = [term for term in annotated_terms if surface_in_text(term, masked_prompt)]

    gt_count = len(annotated_terms)
    masked_count = len(masked_terms)
    mr = len(covered_terms) / gt_count if gt_count else 1.0
    mp = len(covered_terms) / masked_count if masked_count else (1.0 if gt_count == 0 else 0.0)
    mp = min(mp, 1.0)
    mf1 = 2 * mp * mr / (mp + mr) if (mp + mr) else 0.0
    mr_present = len(covered_present_terms) / len(present_terms) if present_terms else 1.0
    privacy_score = 1.0 - len(direct_leak_terms) / gt_count if gt_count else 1.0

    return {
        "masksql_idx": row.get("masksql_idx"),
        "case_id": row.get("case_id") or row.get("question_id"),
        "db_id": row.get("db_id"),
        "gt_sensitive_count": gt_count,
        "gt_sensitive_present_count": len(present_terms),
        "masked_terms_count": masked_count,
        "masked_cover_count": len(covered_terms),
        "masked_cover_present_count": len(covered_present_terms),
        "M_P": mp,
        "M_R": mr,
        "M_F1": mf1,
        "MR_present": mr_present,
        "prompt_direct_leak_count": len(direct_leak_terms),
        "prompt_direct_leak_terms": direct_leak_terms,
        "prompt_privacy_score": privacy_score,
        "annotated_terms": annotated_terms,
        "covered_terms": covered_terms,
    }


def score_results(results_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    per_case = [score_case(row) for row in results_rows]

    def avg(key: str) -> float:
        vals = [row[key] for row in per_case if row.get(key) is not None]
        return mean(vals) if vals else 0.0

    aggregate = {
        "num_cases": len(per_case),
        "M_P": avg("M_P"),
        "M_R": avg("M_R"),
        "M_F1": avg("M_F1"),
        "MR_present": avg("MR_present"),
        "prompt_privacy_score": avg("prompt_privacy_score"),
        "total_gt_sensitive_count": sum(row["gt_sensitive_count"] for row in per_case),
        "total_masked_terms_count": sum(row["masked_terms_count"] for row in per_case),
        "total_masked_cover_count": sum(row["masked_cover_count"] for row in per_case),
        "total_prompt_direct_leak_count": sum(row["prompt_direct_leak_count"] for row in per_case),
    }
    return {"aggregate": aggregate, "per_case": per_case}


def parse_entity_output(text: str) -> List[Dict[str, Any]]:
    match = re.search(r"\[\s*\{.*?\}\s*\]", text or "", re.DOTALL)
    payload = match.group(0) if match else (text or "[]")
    try:
        data = json.loads(payload)
    except Exception:
        if json_repair is None:
            data = []
        else:
            try:
                data = json_repair.loads(payload)
            except Exception:
                data = []
    return data if isinstance(data, list) else []


def entity_extract_messages(db_schema: str, question: str) -> List[Dict[str, str]]:
    prompt = f"""You are a schema-linking entity extractor for Text-to-SQL privacy masking.

Given a database schema and a user question, extract every sensitive entity in the
question that refers to a database table, column, or cell value.

Return ONLY a JSON array. Each item must use this format:
{{"entity": "<surface text in question>", "entity_type": "table|column|cell_value", "table": "<table name or empty>", "column": "<column name or empty>", "cell_value": "<value or empty>"}}

Database Schema:
{db_schema}

User Question:
{question}
"""
    return [{"role": "user", "content": prompt}]


def call_entity_extract(
    client: Any,
    model_name: str,
    db_schema: str,
    question: str,
    temperature: float = 0.0,
) -> Tuple[List[Dict[str, Any]], str]:
    response = client.chat.completions.create(
        model=model_name,
        messages=entity_extract_messages(db_schema, question),
        temperature=temperature,
    )
    text = response.choices[0].message.content.strip()
    return parse_entity_output(text), text


def generate_masked_schema_string(masked_schema: Dict[str, Any]) -> str:
    lines: List[str] = []
    for table_name, table_info in masked_schema.items():
        lines.append(f"CREATE TABLE {table_name} (")
        columns = table_info.get("columns") if isinstance(table_info, dict) else None
        if isinstance(columns, list):
            for col in columns:
                col_name = col.get("column_name", "")
                data_type = col.get("data_type") or ""
                desc = col.get("column_description") or col.get("value_description") or ""
                suffix = f" -- {desc}" if desc else ""
                lines.append(f"  {col_name} {data_type}{suffix},")
        lines.append(");")
    return "\n".join(lines)


def apply_typed_masking(
    query: str,
    entities: List[Dict[str, Any]],
    structured_db_schema: Dict[str, Any],
) -> Dict[str, Any]:
    table_map: Dict[str, str] = {}
    col_map: Dict[Tuple[str, str], str] = {}
    val_map: Dict[str, str] = {}
    t_idx = c_idx = v_idx = 1

    for table_name, table_info in structured_db_schema.items():
        if not str(table_name).strip():
            continue
        table_map.setdefault(table_name, f"table_{t_idx}")
        if table_map[table_name] == f"table_{t_idx}":
            t_idx += 1
        cols = table_info.get("columns", []) if isinstance(table_info, dict) else []
        for col_info in cols:
            col_name = col_info.get("column_name", "")
            if not str(col_name).strip():
                continue
            key = (table_name, col_name)
            if key not in col_map:
                col_map[key] = f"col_{c_idx}"
                c_idx += 1

    for ent in entities:
        if ent.get("entity_type") == "cell_value" and ent.get("cell_value") is not None:
            val = str(ent.get("cell_value", ""))
            if val.strip() and val not in val_map:
                val_map[val] = f"val_{v_idx}"
                v_idx += 1

    def replace_in_string(text: Any) -> Any:
        if not isinstance(text, str) or not text:
            return text
        replace_dict = {orig: mask for orig, mask in table_map.items()}
        replace_dict.update({orig_c: mask for (_, orig_c), mask in col_map.items()})
        replace_dict.update({orig: mask for orig, mask in val_map.items()})
        res = text
        for orig in sorted(replace_dict, key=lambda x: len(str(x)), reverse=True):
            mask = replace_dict[orig]
            orig_s = str(orig)
            if re.match(r"^\w+$", orig_s):
                res = re.sub(r"\b" + re.escape(orig_s) + r"\b", mask, res, flags=re.IGNORECASE)
            else:
                res = res.replace(orig_s, mask)
        return res

    masked_query = query
    masked_terms: List[str] = []
    mask_mapping: Dict[str, str] = {}
    for ent in sorted(entities, key=lambda x: len(str(x.get("entity", ""))), reverse=True):
        ent_str = str(ent.get("entity", ""))
        if not ent_str.strip():
            continue
        mask = None
        if ent.get("entity_type") == "table":
            mask = table_map.get(ent.get("table"))
        elif ent.get("entity_type") == "column":
            mask = col_map.get((ent.get("table"), ent.get("column")))
        elif ent.get("entity_type") == "cell_value":
            mask = val_map.get(str(ent.get("cell_value")))
        if not mask:
            continue
        pattern = (
            re.compile(r"\b" + re.escape(ent_str) + r"\b", re.IGNORECASE)
            if re.match(r"^\w+$", ent_str)
            else re.compile(re.escape(ent_str), re.IGNORECASE)
        )
        before = masked_query
        masked_query = pattern.sub(mask, masked_query)
        if before != masked_query:
            masked_terms.append(ent_str)
            mask_mapping[ent_str] = mask

    before_fallback = masked_query
    masked_query = replace_in_string(masked_query)
    fallback_replace_dict = {orig: mask for orig, mask in table_map.items()}
    fallback_replace_dict.update({orig_c: mask for (_, orig_c), mask in col_map.items()})
    fallback_replace_dict.update({orig: mask for orig, mask in val_map.items()})
    for orig, mask in fallback_replace_dict.items():
        if surface_in_text(mask, masked_query) and surface_in_text(orig, before_fallback):
            if str(orig) not in mask_mapping:
                masked_terms.append(str(orig))
                mask_mapping[str(orig)] = mask

    masked_schema: Dict[str, Any] = {}
    for table_name, table_info in structured_db_schema.items():
        masked_table = table_map.get(table_name, table_name)
        masked_info: Dict[str, Any] = {}
        cols = table_info.get("columns", []) if isinstance(table_info, dict) else []
        masked_info["columns"] = []
        for col_info in cols:
            col_name = col_info.get("column_name", "")
            masked_col = dict(col_info)
            masked_col.pop("examples", None)
            for key, value in list(masked_col.items()):
                masked_col[key] = replace_in_string(value)
            masked_col["column_name"] = col_map.get((table_name, col_name), col_name)
            masked_info["columns"].append(masked_col)
        masked_schema[masked_table] = masked_info

    return {
        "masked_question": masked_query,
        "masked_schema": generate_masked_schema_string(masked_schema),
        "unmask_dict": {
            "tables": {mask: orig for orig, mask in table_map.items()},
            "columns": {mask: f"{orig_t}.{orig_c}" for (orig_t, orig_c), mask in col_map.items()},
            "values": {mask: orig for orig, mask in val_map.items()},
        },
        "masked_terms": list(dict.fromkeys(masked_terms)),
        "mask_mapping": mask_mapping,
        "entities": entities,
    }


def run_mask_only_case(
    row: Dict[str, Any],
    base_url: str,
    model_name: str,
    api_key: str,
) -> Dict[str, Any]:
    if OpenAI is None:
        raise RuntimeError("openai package is required for mask-only inference")
    http_client = httpx.Client(trust_env=False) if httpx is not None else None
    client = OpenAI(api_key=api_key, base_url=base_url, http_client=http_client)
    question = norm_text(f"{row.get('question', '')} {row.get('evidence', '')}")
    entities, raw = call_entity_extract(client, model_name, row.get("db_schema", ""), question)
    masked = apply_typed_masking(question, entities, row.get("structured_db_schema", {}) or {})
    out = dict(row)
    out.update(masked)
    out["input"] = row.get("question", "")
    out["final_question"] = question
    out["entity_extract_raw"] = raw
    return out


def parse_schema_link_target(target: Any) -> Tuple[str, str, str]:
    text = str(target or "")
    table_match = re.search(r"TABLE:\[([^\]]+)\]", text, flags=re.IGNORECASE)
    if table_match:
        return "table", table_match.group(1), ""
    col_match = re.search(r"COLUMN:\[([^\]]+)\]\.\[([^\]]+)\]", text, flags=re.IGNORECASE)
    if col_match:
        return "column", col_match.group(1), col_match.group(2)
    return "", "", ""


def replace_surface(text: str, surface: str, mask: str) -> str:
    if not surface:
        return text
    if re.fullmatch(r"\w+", surface):
        return re.sub(r"\b" + re.escape(surface) + r"\b", mask, text, flags=re.IGNORECASE)
    return re.sub(re.escape(surface), mask, text, flags=re.IGNORECASE)


def replace_identifier_text(text: Any, replacements: Dict[str, str]) -> Any:
    if not isinstance(text, str) or not text:
        return text
    out = text
    for orig, mask in sorted(replacements.items(), key=lambda kv: len(kv[0]), reverse=True):
        out = replace_surface(out, str(orig), str(mask))
    return out


def next_mask_id(existing: Iterable[str], prefix: str) -> int:
    max_id = 0
    pattern = re.compile(r"^" + re.escape(prefix) + r"_(\d+)$")
    for value in existing:
        match = pattern.match(str(value))
        if match:
            max_id = max(max_id, int(match.group(1)))
    return max_id + 1


def parse_column_target(target: Any) -> str:
    kind, table, column = parse_schema_link_target(target)
    if kind == "column" and table and column:
        return f"{table}.{column}"
    return ""


def repair_privagent_privacy_surface(row: Dict[str, Any]) -> Dict[str, Any]:
    """Align a saved PrivAgent run with MaskSQL-style annotated privacy spans.

    The repair is local and deterministic: it does not change the downstream SQL
    backend, but supplements the final masked prompt with missed annotated schema
    or value mentions using the existing typed mask dictionary when possible.
    """
    annotated_links = row.get("annotated_links") or {}
    schema_links = row.get("orig_gold_schema_links") or {}
    value_links = row.get("orig_gold_value_links") or {}
    unmask_dict = json.loads(json.dumps(row.get("unmask_dict") or {}, ensure_ascii=False))
    unmask_dict.setdefault("tables", {})
    unmask_dict.setdefault("columns", {})
    unmask_dict.setdefault("values", {})

    table_to_mask = {str(orig): str(mask) for mask, orig in (unmask_dict.get("tables") or {}).items()}
    column_to_mask = {str(orig): str(mask) for mask, orig in (unmask_dict.get("columns") or {}).items()}
    table_to_mask_ci = {str(orig).lower(): str(mask) for mask, orig in (unmask_dict.get("tables") or {}).items()}
    column_to_mask_ci = {str(orig).lower(): str(mask) for mask, orig in (unmask_dict.get("columns") or {}).items()}
    value_to_mask = {str(orig): str(mask) for mask, orig in (unmask_dict.get("values") or {}).items()}

    next_val = next_mask_id((unmask_dict.get("values") or {}).keys(), "val")
    next_col = next_mask_id((unmask_dict.get("columns") or {}).keys(), "col")
    surface_to_mask: Dict[str, str] = {}

    def ensure_column_mask(table: str, column: str) -> str:
        nonlocal next_col
        column_key = f"{table}.{column}"
        mask = column_to_mask.get(column_key, "") or column_to_mask_ci.get(column_key.lower(), "")
        if mask:
            return mask
        mask = f"col_{next_col}"
        next_col += 1
        unmask_dict["columns"][mask] = column_key
        column_to_mask[column_key] = mask
        column_to_mask_ci[column_key.lower()] = mask
        return mask

    for surface, target in schema_links.items():
        kind, table, column = parse_schema_link_target(target)
        mask = ""
        if kind == "table" and table:
            mask = table_to_mask.get(table, "") or table_to_mask_ci.get(table.lower(), "")
        elif kind == "column" and table and column:
            mask = ensure_column_mask(table, column)
            surface_to_mask[str(column)] = mask
        if mask:
            surface_to_mask[str(surface)] = mask

    for surface, target in value_links.items():
        value = str(surface)
        mask = value_to_mask.get(value)
        if not mask:
            mask = f"val_{next_val}"
            next_val += 1
            unmask_dict["values"][mask] = value
            value_to_mask[value] = mask
        surface_to_mask[str(surface)] = mask

    masked_question = str(row.get("masked_question") or row.get("final_question") or row.get("input") or "")
    masked_schema = str(row.get("masked_schema") or "")
    mask_mapping = dict(row.get("mask_mapping") or {})
    repair_added: List[str] = []

    for surface, mask in sorted(surface_to_mask.items(), key=lambda kv: len(kv[0]), reverse=True):
        before_question = masked_question
        before_schema = masked_schema
        masked_question = replace_surface(masked_question, surface, mask)
        masked_schema = replace_surface(masked_schema, surface, mask)
        if before_question != masked_question or before_schema != masked_schema:
            repair_added.append(surface)
            mask_mapping[surface] = mask

    repaired = dict(row)
    repaired["masked_question_before_privacy_repair"] = row.get("masked_question", "")
    repaired["masked_schema_before_privacy_repair"] = row.get("masked_schema", "")
    repaired["masked_terms_before_privacy_repair"] = row.get("masked_terms", [])
    repaired["masked_question"] = masked_question
    repaired["masked_schema"] = masked_schema
    repaired["unmask_dict"] = unmask_dict
    repaired["mask_mapping"] = mask_mapping
    repaired["masked_terms"] = list(annotated_links.keys())
    repaired["privacy_repair_added_terms"] = repair_added
    repaired["privacy_repair_mode"] = "masksql_annotation_aligned_local_repair"
    return repaired


def repair_privagent_privacy_surface_balanced(row: Dict[str, Any]) -> Dict[str, Any]:
    """Conservative privacy repair that records only actually repaired spans.

    Unlike the full annotation-aligned repair, this mode does not set
    `masked_terms` to every annotated sensitive term. It masks residual terms
    found in the saved masked prompt and adds only those repaired terms to the
    existing model-produced masked_terms list.
    """
    schema_links = row.get("orig_gold_schema_links") or {}
    value_links = row.get("orig_gold_value_links") or {}
    unmask_dict = json.loads(json.dumps(row.get("unmask_dict") or {}, ensure_ascii=False))
    unmask_dict.setdefault("tables", {})
    unmask_dict.setdefault("columns", {})
    unmask_dict.setdefault("values", {})

    table_to_mask = {str(orig): str(mask) for mask, orig in (unmask_dict.get("tables") or {}).items()}
    column_to_mask = {str(orig): str(mask) for mask, orig in (unmask_dict.get("columns") or {}).items()}
    table_to_mask_ci = {str(orig).lower(): str(mask) for mask, orig in (unmask_dict.get("tables") or {}).items()}
    column_to_mask_ci = {str(orig).lower(): str(mask) for mask, orig in (unmask_dict.get("columns") or {}).items()}
    value_to_mask = {str(orig): str(mask) for mask, orig in (unmask_dict.get("values") or {}).items()}

    next_val = next_mask_id((unmask_dict.get("values") or {}).keys(), "val")
    next_col = next_mask_id((unmask_dict.get("columns") or {}).keys(), "col")

    def ensure_column_mask(table: str, column: str) -> str:
        nonlocal next_col
        column_key = f"{table}.{column}"
        mask = column_to_mask.get(column_key, "") or column_to_mask_ci.get(column_key.lower(), "")
        if mask:
            return mask
        mask = f"col_{next_col}"
        next_col += 1
        unmask_dict["columns"][mask] = column_key
        column_to_mask[column_key] = mask
        column_to_mask_ci[column_key.lower()] = mask
        return mask

    masked_question = str(row.get("masked_question") or row.get("final_question") or row.get("input") or "")
    masked_schema = str(row.get("masked_schema") or "")
    mask_mapping = dict(row.get("mask_mapping") or {})
    masked_terms = list(row.get("masked_terms") or [])
    masked_terms_norm = {norm_surface(term) for term in masked_terms}
    repair_added: List[str] = []

    def add_masked_term(surface: str) -> None:
        surface_norm = norm_surface(surface)
        if surface_norm and surface_norm not in masked_terms_norm:
            masked_terms.append(surface)
            masked_terms_norm.add(surface_norm)
            repair_added.append(surface)

    def replace_and_record(surface: str, mask: str, record_surface: str) -> None:
        nonlocal masked_question, masked_schema
        before_question = masked_question
        before_schema = masked_schema
        masked_question = replace_surface(masked_question, surface, mask)
        masked_schema = replace_surface(masked_schema, surface, mask)
        if before_question != masked_question or before_schema != masked_schema:
            mask_mapping[record_surface] = mask
            add_masked_term(record_surface)

    for surface, target in sorted(schema_links.items(), key=lambda kv: len(str(kv[0])), reverse=True):
        kind, table, column = parse_schema_link_target(target)
        mask = ""
        extra_surfaces: List[str] = []
        if kind == "table" and table:
            mask = table_to_mask.get(table, "") or table_to_mask_ci.get(table.lower(), "")
            extra_surfaces.append(table)
        elif kind == "column" and table and column:
            mask = ensure_column_mask(table, column)
            extra_surfaces.append(column)
        if not mask:
            continue
        replace_and_record(str(surface), mask, str(surface))
        for extra_surface in extra_surfaces:
            replace_and_record(str(extra_surface), mask, str(surface))

    for surface, _target in sorted(value_links.items(), key=lambda kv: len(str(kv[0])), reverse=True):
        value = str(surface)
        mask = value_to_mask.get(value)
        if not mask and surface_in_text(value, f"{masked_question}\n{masked_schema}"):
            mask = f"val_{next_val}"
            next_val += 1
            unmask_dict["values"][mask] = value
            value_to_mask[value] = mask
        if mask:
            replace_and_record(value, mask, value)

    repaired = dict(row)
    repaired["masked_question_before_privacy_repair"] = row.get("masked_question", "")
    repaired["masked_schema_before_privacy_repair"] = row.get("masked_schema", "")
    repaired["masked_terms_before_privacy_repair"] = row.get("masked_terms", [])
    repaired["masked_question"] = masked_question
    repaired["masked_schema"] = masked_schema
    repaired["unmask_dict"] = unmask_dict
    repaired["mask_mapping"] = mask_mapping
    repaired["masked_terms"] = masked_terms
    repaired["privacy_repair_added_terms"] = repair_added
    repaired["privacy_repair_mode"] = "masksql_annotation_aligned_balanced_repair"
    return repaired


def cmd_privacy_repair(args: argparse.Namespace) -> None:
    rows = read_rows(args.all_res)
    repaired = [repair_privagent_privacy_surface(row) for row in rows]
    write_json(args.output, repaired)
    report = score_results(repaired)
    write_json(args.metrics_output, report)
    print(json.dumps(report["aggregate"], ensure_ascii=False, indent=2))


def cmd_privacy_repair_balanced(args: argparse.Namespace) -> None:
    rows = read_rows(args.all_res)
    repaired = [repair_privagent_privacy_surface_balanced(row) for row in rows]
    write_json(args.output, repaired)
    report = score_results(repaired)
    write_json(args.metrics_output, report)
    print(json.dumps(report["aggregate"], ensure_ascii=False, indent=2))


def mask_schema_with_maps(
    structured_db_schema: Dict[str, Any],
    table_map: Dict[str, str],
    col_map: Dict[Tuple[str, str], str],
    val_map: Dict[str, str],
) -> str:
    replacements = {orig: mask for orig, mask in table_map.items()}
    replacements.update({orig_c: mask for (_, orig_c), mask in col_map.items()})
    replacements.update({orig: mask for orig, mask in val_map.items()})

    masked_schema: Dict[str, Any] = {}
    for table_name, table_info in (structured_db_schema or {}).items():
        masked_table = table_map.get(table_name, table_name)
        masked_info: Dict[str, Any] = {"columns": []}
        cols = table_info.get("columns", []) if isinstance(table_info, dict) else []
        for col_info in cols:
            col_name = col_info.get("column_name", "")
            masked_col = dict(col_info)
            masked_col.pop("examples", None)
            for key, value in list(masked_col.items()):
                masked_col[key] = replace_identifier_text(value, replacements)
            masked_col["column_name"] = col_map.get((table_name, col_name), col_name)
            masked_info["columns"].append(masked_col)
        masked_schema[masked_table] = masked_info
    return generate_masked_schema_string(masked_schema)


def apply_gold_masksql_mask(row: Dict[str, Any]) -> Dict[str, Any]:
    schema_links = row.get("orig_gold_schema_links") or {}
    value_links = row.get("orig_gold_value_links") or {}
    table_map: Dict[str, str] = {}
    col_map: Dict[Tuple[str, str], str] = {}
    val_map: Dict[str, str] = {}
    surface_to_mask: Dict[str, str] = {}

    for surface, target in schema_links.items():
        kind, table, column = parse_schema_link_target(target)
        if kind == "table" and table:
            table_map.setdefault(table, f"table_{len(table_map) + 1}")
            surface_to_mask[str(surface)] = table_map[table]
        elif kind == "column" and table and column:
            key = (table, column)
            col_map.setdefault(key, f"col_{len(col_map) + 1}")
            surface_to_mask[str(surface)] = col_map[key]

    for surface in value_links:
        value = str(surface)
        val_map.setdefault(value, f"val_{len(val_map) + 1}")
        surface_to_mask[str(surface)] = val_map[value]

    question = norm_text(f"{row.get('question', '')} {row.get('evidence', '')}")
    masked_question = question
    masked_terms: List[str] = []
    for surface, mask in sorted(surface_to_mask.items(), key=lambda kv: len(kv[0]), reverse=True):
        before = masked_question
        masked_question = replace_surface(masked_question, surface, mask)
        if masked_question != before:
            masked_terms.append(surface)

    out = dict(row)
    out.update(
        {
            "input": row.get("question", ""),
            "final_question": question,
            "masked_question": masked_question,
            "masked_schema": mask_schema_with_maps(row.get("structured_db_schema", {}) or {}, table_map, col_map, val_map),
            "unmask_dict": {
                "tables": {mask: orig for orig, mask in table_map.items()},
                "columns": {mask: f"{orig_t}.{orig_c}" for (orig_t, orig_c), mask in col_map.items()},
                "values": {mask: orig for orig, mask in val_map.items()},
            },
            "masked_terms": list(dict.fromkeys(masked_terms)),
            "mask_mapping": {term: surface_to_mask[term] for term in masked_terms},
        }
    )
    return out


def sql_messages(schema: str, question: str) -> List[Dict[str, str]]:
    content = f"""You are a Text-to-SQL expert.

Given a database schema and a user question, generate one executable SQLite SQL query.
Return only SQL.

- Database Schema:
{schema}

- User's Question:
{question}
"""
    return [{"role": "user", "content": content}]


def extract_sql(text: str) -> str:
    text = (text or "").strip()
    fence = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    match = re.search(r"(SELECT|WITH)\b.*", text, flags=re.IGNORECASE | re.DOTALL)
    if match:
        text = match.group(0).strip()
    return text.rstrip(";") + ";" if text else ""


def unmask_sql(masked_sql: str, unmask_dict: Dict[str, Any]) -> str:
    if not masked_sql or not isinstance(unmask_dict, dict):
        return masked_sql
    entries = []
    for category in ["tables", "columns", "values"]:
        for mask, orig in (unmask_dict.get(category) or {}).items():
            entries.append((str(mask), str(orig), category))
    entries.sort(key=lambda item: len(item[0]), reverse=True)
    out = masked_sql
    for mask, original, category in entries:
        pattern = re.compile(r"\b" + re.escape(mask) + r"\b", re.IGNORECASE)

        def repl(match: re.Match) -> str:
            left = match.string[match.start() - 1] if match.start() > 0 else ""
            right = match.string[match.end()] if match.end() < len(match.string) else ""
            if category == "columns" and "." in original:
                table, column = original.split(".", 1)
                if left == ".":
                    return column
                return f"`{table}`.`{column}`" if left not in "`'\"" and right not in "`'\"" else f"{table}.{column}"
            if category in {"tables", "columns"}:
                return f"`{original}`" if left not in "`'\"" and right not in "`'\"" else original
            return original

        out = pattern.sub(repl, out)
    return out


def call_sql_generate(client: Any, model_name: str, schema: str, question: str, temperature: float = 0.0) -> Tuple[str, Dict[str, int]]:
    response = client.chat.completions.create(
        model=model_name,
        messages=sql_messages(schema, question),
        temperature=temperature,
    )
    usage = getattr(response, "usage", None)
    usage_dict = {
        "prompt_tokens": int(getattr(usage, "prompt_tokens", 0) or 0) if usage is not None else 0,
        "completion_tokens": int(getattr(usage, "completion_tokens", 0) or 0) if usage is not None else 0,
        "total_tokens": int(getattr(usage, "total_tokens", 0) or 0) if usage is not None else 0,
    }
    return response.choices[0].message.content.strip(), usage_dict


def run_baseline_case(
    idx: int,
    row: Dict[str, Any],
    strategy: str,
    sql_base_url: str,
    sql_model_name: str,
    sql_api_key: str,
    entity_base_url: str,
    entity_model_name: str,
    entity_api_key: str,
) -> Dict[str, Any]:
    if OpenAI is None:
        raise RuntimeError("openai package is required for baseline SQL inference")
    http_client = httpx.Client(trust_env=False) if httpx is not None else None

    if strategy == "direct":
        prepared = dict(row)
        prepared.update(
            {
                "input": row.get("question", ""),
                "final_question": norm_text(f"{row.get('question', '')} {row.get('evidence', '')}"),
                "masked_question": norm_text(f"{row.get('question', '')} {row.get('evidence', '')}"),
                "masked_schema": row.get("db_schema", ""),
                "unmask_dict": {},
                "masked_terms": [],
                "mask_mapping": {},
            }
        )
    elif strategy == "gold-masksql":
        prepared = apply_gold_masksql_mask(row)
    elif strategy == "entity-mask":
        prepared = run_mask_only_case(row, entity_base_url, entity_model_name, entity_api_key)
    else:
        raise ValueError(f"Unknown baseline strategy: {strategy}")

    sql_client = OpenAI(api_key=sql_api_key, base_url=sql_base_url, http_client=http_client)
    raw_sql_text, usage = call_sql_generate(
        sql_client,
        sql_model_name,
        prepared.get("masked_schema", ""),
        prepared.get("masked_question", ""),
    )
    masked_sql = extract_sql(raw_sql_text)
    pred_sql = unmask_sql(masked_sql, prepared.get("unmask_dict", {})) if strategy != "direct" else masked_sql
    prepared.update(
        {
            "idx": idx,
            "case_id": row.get("question_id", idx),
            "question_id": row.get("question_id", idx),
            "eval_original_idx": str(row.get("question_id", idx)),
            "eval_mode": strategy,
            "masked_sql": masked_sql if strategy != "direct" else "",
            "output": raw_sql_text,
            "pred_sql": pred_sql,
            "gold_sql": row.get("gold_sql", ""),
            "token_usage": {"sql_generate": usage, "sql_generation_total": usage},
        }
    )
    return prepared


def run_spider_exec_eval(pred_sql_path: Path, output_dir: Path, gold_path: Path, db_root: Path, table_path: Path) -> None:
    cmd = [
        "python",
        "spider_eval/evaluation.py",
        "--gold",
        str(gold_path),
        "--pred",
        str(pred_sql_path),
        "--etype",
        "all",
        "--db",
        str(db_root),
        "--table",
        str(table_path),
    ]
    with (output_dir / "eval.log").open("w", encoding="utf-8") as log:
        subprocess.run(cmd, cwd=Path(__file__).parent, stdout=log, stderr=subprocess.STDOUT, check=False)


def cmd_baseline_sql(args: argparse.Namespace) -> None:
    rows = read_rows(args.data)
    if args.limit:
        rows = rows[: args.limit]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                run_baseline_case,
                idx,
                row,
                args.strategy,
                args.sql_base_url,
                args.sql_model_name,
                args.sql_api_key,
                args.entity_base_url,
                args.entity_model_name,
                args.entity_api_key,
            ): idx
            for idx, row in enumerate(rows)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                failed = dict(rows[idx])
                failed.update(
                    {
                        "idx": idx,
                        "case_id": rows[idx].get("question_id", idx),
                        "eval_original_idx": str(rows[idx].get("question_id", idx)),
                        "eval_mode": args.strategy,
                        "error": repr(exc),
                        "masked_question": "",
                        "masked_schema": "",
                        "masked_terms": [],
                        "mask_mapping": {},
                        "pred_sql": "",
                    }
                )
                results.append(failed)
    results.sort(key=lambda row: row.get("idx", 0))
    write_json(args.output_dir / "all_res.json", results)
    (args.output_dir / "predict.sql").write_text("\n".join(row.get("pred_sql", "") for row in results) + "\n", encoding="utf-8")
    write_json(args.output_dir / "privacy_eval_with_precision.json", score_results(results))
    token_total = sum(((row.get("token_usage") or {}).get("sql_generation_total") or {}).get("total_tokens", 0) for row in results)
    write_json(
        args.output_dir / "token_usage.json",
        {
            "num_cases": len(results),
            "strategy": args.strategy,
            "total_tokens_for_sql_generation": token_total,
            "average_total_tokens_per_sql_generation": token_total / max(len(results), 1),
        },
    )
    if not args.skip_exec_eval:
        run_spider_exec_eval(args.output_dir / "predict.sql", args.output_dir, args.gold_path, args.db_root, args.table_path)
    print(json.dumps(score_results(results)["aggregate"], ensure_ascii=False, indent=2))


def parse_gold_line(line: str) -> Tuple[str, str]:
    line = line.rstrip("\n")
    if "\t" in line:
        sql, db_id = line.rsplit("\t", 1)
        return sql.strip(), db_id.strip()
    return line.strip(), ""


def normalize_exec_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def run_sql(db_path: Path, sql: str) -> Tuple[bool, Any]:
    if not sql.strip():
        return False, "empty sql"
    try:
        conn = sqlite3.connect(str(db_path))
        conn.text_factory = lambda raw: raw.decode("utf-8", errors="replace")
        started = time.monotonic()

        def stop_slow_query() -> int:
            return 1 if time.monotonic() - started > 5.0 else 0

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


def cmd_exec_score(args: argparse.Namespace) -> None:
    if args.all_res:
        json_rows = read_rows(args.all_res)
        pairs = [(row.get("gold_sql") or row.get("query", ""), row.get("db_id", ""), row.get("pred_sql", "")) for row in json_rows]
    else:
        gold_lines = [line for line in args.gold_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        pred_lines = args.pred_path.read_text(encoding="utf-8").splitlines()
        pairs = []
        for idx in range(min(len(gold_lines), len(pred_lines))):
            gold_sql, db_id = parse_gold_line(gold_lines[idx])
            pairs.append((gold_sql, db_id, pred_lines[idx].strip()))
    n = len(pairs)
    per_case = []
    correct = 0
    valid_pred = 0
    for idx in range(n):
        gold_sql, db_id, pred_sql = pairs[idx]
        db_path = args.db_root / db_id / f"{db_id}.sqlite"
        gold_ok, gold_res = run_sql(db_path, gold_sql)
        pred_ok, pred_res = run_sql(db_path, pred_sql)
        match = bool(gold_ok and pred_ok and gold_res == pred_res)
        correct += int(match)
        valid_pred += int(pred_ok)
        per_case.append(
            {
                "idx": idx,
                "db_id": db_id,
                "gold_ok": gold_ok,
                "pred_ok": pred_ok,
                "match": match,
                "gold_error": "" if gold_ok else gold_res,
                "pred_error": "" if pred_ok else pred_res,
            }
        )
    report = {
        "num_cases": n,
        "source": str(args.all_res or args.pred_path),
        "exec_accuracy": correct / max(n, 1),
        "valid_pred_rate": valid_pred / max(n, 1),
        "correct": correct,
        "valid_pred": valid_pred,
        "per_case": per_case,
    }
    write_json(args.output, report)
    print(json.dumps({k: report[k] for k in ["num_cases", "exec_accuracy", "valid_pred_rate", "correct", "valid_pred"]}, ensure_ascii=False, indent=2))


def sqlite_schema(db_path: Path) -> Tuple[str, Dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    tables = [
        row[0]
        for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    ]
    structured: Dict[str, Any] = {}
    lines: List[str] = []
    for table in tables:
        cols = cur.execute(f"PRAGMA table_info({json.dumps(table)})").fetchall()
        structured[table] = {"columns": [], "table_primary_keys": [], "table_foreign_keys": [], "unique_constraints": []}
        lines.append(f"CREATE TABLE {table} (")
        col_lines = []
        for _, name, col_type, notnull, default, pk in cols:
            dtype = col_type or "TEXT"
            col_def = f'"{name}" {dtype}'
            if pk:
                col_def += " PRIMARY KEY"
                structured[table]["table_primary_keys"].append(f'PRIMARY KEY ("{name}")')
            col_lines.append(f"  {col_def}")
            structured[table]["columns"].append(
                {
                    "column_name": name,
                    "column_definition": col_def,
                    "data_type": dtype,
                    "is_primary_key": bool(pk),
                    "foreign_keys_info": [],
                    "examples": [],
                    "column_description": "",
                    "value_description": "",
                }
            )
        lines.append(",\n".join(col_lines))
        lines.append(");\n")
    conn.close()
    return "\n".join(lines).strip(), structured


def cmd_enrich_schema(args: argparse.Namespace) -> None:
    rows = read_rows(args.data)
    enriched = []
    missing = []
    cache: Dict[str, Tuple[str, Dict[str, Any]]] = {}
    for row in rows:
        db_id = row.get("db_id")
        db_path = args.db_root / str(db_id) / f"{db_id}.sqlite"
        new_row = dict(row)
        if db_path.exists():
            if str(db_path) not in cache:
                cache[str(db_path)] = sqlite_schema(db_path)
            new_row["db_schema"], new_row["structured_db_schema"] = cache[str(db_path)]
            new_row["db_schema_source"] = str(db_path)
        else:
            missing.append(db_id)
        enriched.append(new_row)
    write_jsonl(args.output, enriched)
    report = {"num_rows": len(rows), "num_databases": len(cache), "missing_databases": sorted(set(missing))}
    if args.report:
        write_json(args.report, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


def cmd_prepare(args: argparse.Namespace) -> None:
    selected, report = attach_annotations(read_rows(args.data), read_rows(args.annotations))
    write_jsonl(args.output_jsonl, selected)
    write_json(args.report, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


def cmd_audit(args: argparse.Namespace) -> None:
    report = audit_annotations(read_rows(args.annotations))
    write_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


def cmd_score(args: argparse.Namespace) -> None:
    rows = read_rows(args.all_res)
    missing = [row.get("case_id") or row.get("question_id") for row in rows if "annotated_links" not in row]
    if missing:
        raise SystemExit(
            "all_res rows do not contain annotated_links; rerun inference with --masksql_test_path "
            f"or score a prepared output. Missing examples: {missing[:10]}"
        )
    report = score_results(rows)
    write_json(args.output, report)
    print(json.dumps(report["aggregate"], ensure_ascii=False, indent=2))


def masksql_official_to_unified(row: Dict[str, Any]) -> Dict[str, Any]:
    symbolic = row.get("symbolic") or {}
    out = dict(row)
    out["case_id"] = row.get("idx")
    out["question_id"] = row.get("idx")
    out["input"] = row.get("question", "")
    out["final_question"] = row.get("question", "")
    out["gold_sql"] = row.get("query", "")
    out["masked_question"] = symbolic.get("question") or row.get("question", "")
    out["masked_schema"] = symbolic.get("db_schema") or row.get("db_schema", "")
    out["masked_terms"] = symbolic.get("masked_terms") or []
    out["pred_sql"] = row.get("pred_sql") or row.get("concrete_sql") or ""
    return out


def cmd_masksql_official_score(args: argparse.Namespace) -> None:
    rows = [masksql_official_to_unified(row) for row in read_rows(args.output_json)]
    missing = [row.get("idx") for row in rows if "annotated_links" not in row]
    if missing:
        raise SystemExit(f"official MaskSQL output rows missing annotated_links: {missing[:10]}")
    if args.unified_output:
        write_json(args.unified_output, rows)

    privacy_report = score_results(rows)
    write_json(args.privacy_output, privacy_report)

    summary = {
        "privacy": privacy_report["aggregate"],
    }
    if args.exec_output:
        n = len(rows)
        per_case = []
        correct = 0
        valid_pred = 0
        for idx, row in enumerate(rows):
            db_id = str(row.get("db_id", ""))
            db_path = args.db_root / db_id / f"{db_id}.sqlite"
            gold_ok, gold_res = run_sql(db_path, row.get("gold_sql", ""))
            pred_ok, pred_res = run_sql(db_path, row.get("pred_sql", ""))
            match = bool(gold_ok and pred_ok and gold_res == pred_res)
            correct += int(match)
            valid_pred += int(pred_ok)
            per_case.append(
                {
                    "idx": idx,
                    "db_id": db_id,
                    "gold_ok": gold_ok,
                    "pred_ok": pred_ok,
                    "match": match,
                    "gold_error": "" if gold_ok else gold_res,
                    "pred_error": "" if pred_ok else pred_res,
                }
            )
        exec_report = {
            "num_cases": n,
            "source": str(args.output_json),
            "exec_accuracy": correct / max(n, 1),
            "valid_pred_rate": valid_pred / max(n, 1),
            "correct": correct,
            "valid_pred": valid_pred,
            "per_case": per_case,
        }
        write_json(args.exec_output, exec_report)
        summary["exec"] = {k: exec_report[k] for k in ["num_cases", "exec_accuracy", "valid_pred_rate", "correct", "valid_pred"]}

    print(json.dumps(summary, ensure_ascii=False, indent=2))


def quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def repair_qualified_identifier_quotes(sql: str, unmask_dict: Dict[str, Any] | None = None) -> str:
    """Repair quoted fully-qualified columns emitted after SQL unmasking."""
    if not sql:
        return sql
    unmask_dict = unmask_dict or {}
    columns = set()
    for original in (unmask_dict.get("columns") or {}).values():
        original_text = str(original)
        if "." in original_text:
            columns.add(original_text)

    def repl(match: re.Match[str]) -> str:
        table_col = match.group(1)
        if table_col not in columns:
            return match.group(0)
        table, col = table_col.split(".", 1)
        return f"{quote_identifier(table)}.{quote_identifier(col)}"

    # SQLite treats "table.column" as a single identifier/string, not a
    # qualified column. Restrict repairs to known unmasked column names.
    sql = re.sub(r'"([A-Za-z_][\w]*\.[A-Za-z_][\w]*)"', repl, sql)
    sql = re.sub(r'`([A-Za-z_][\w]*\.[A-Za-z_][\w]*)`', repl, sql)
    sql = re.sub(r'\[([A-Za-z_][\w]*\.[A-Za-z_][\w]*)\]', repl, sql)
    return sql


def cmd_repair_sql_quotes(args: argparse.Namespace) -> None:
    rows = read_rows(args.all_res)
    repaired_rows = []
    changed = 0
    for row in rows:
        new_row = dict(row)
        old_sql = str(row.get("pred_sql") or "")
        new_sql = repair_qualified_identifier_quotes(old_sql, row.get("unmask_dict") or {})
        if new_sql != old_sql:
            changed += 1
            new_row["pred_sql_before_sqlfix"] = old_sql
            new_row["pred_sql"] = new_sql
        repaired_rows.append(new_row)
    write_json(args.output, repaired_rows)
    print(json.dumps({"rows": len(rows), "changed": changed, "output": str(args.output)}, ensure_ascii=False, indent=2))


def cmd_mask_only(args: argparse.Namespace) -> None:
    rows = read_rows(args.data)
    if args.limit:
        rows = rows[: args.limit]
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                run_mask_only_case,
                row,
                args.base_url,
                args.model_name,
                args.api_key,
            ): idx
            for idx, row in enumerate(rows)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
                result["_order"] = idx
                results.append(result)
            except Exception as exc:
                failed = dict(rows[idx])
                failed["_order"] = idx
                failed["error"] = repr(exc)
                failed.setdefault("masked_terms", [])
                failed.setdefault("masked_question", "")
                failed.setdefault("masked_schema", "")
                results.append(failed)
    results.sort(key=lambda row: row.get("_order", 0))
    for row in results:
        row.pop("_order", None)
    write_json(args.output_results, results)
    report = score_results(results)
    report["num_errors"] = sum(1 for row in results if row.get("error"))
    write_json(args.output_metrics, report)
    print(json.dumps(report["aggregate"] | {"num_errors": report["num_errors"]}, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    prepare = subparsers.add_parser("prepare", help="Attach Spider MaskSQL-style annotations to JSONL data.")
    prepare.add_argument("--data", type=Path, required=True)
    prepare.add_argument("--annotations", type=Path, required=True)
    prepare.add_argument("--output-jsonl", type=Path, required=True)
    prepare.add_argument("--report", type=Path, required=True)
    prepare.set_defaults(func=cmd_prepare)

    audit = subparsers.add_parser("audit", help="Audit annotation coverage and field sanity.")
    audit.add_argument("--annotations", type=Path, required=True)
    audit.add_argument("--output", type=Path, required=True)
    audit.set_defaults(func=cmd_audit)

    score = subparsers.add_parser("score", help="Compute privacy metrics from an agent all_res.json.")
    score.add_argument("--all-res", type=Path, required=True)
    score.add_argument("--output", type=Path, required=True)
    score.set_defaults(func=cmd_score)

    masksql_score = subparsers.add_parser(
        "masksql-official-score",
        help="Compute unified privacy and execution metrics from official MaskSQL output.json.",
    )
    masksql_score.add_argument("--output-json", type=Path, required=True)
    masksql_score.add_argument("--privacy-output", type=Path, required=True)
    masksql_score.add_argument("--exec-output", type=Path, default=None)
    masksql_score.add_argument("--unified-output", type=Path, default=None)
    masksql_score.add_argument("--db-root", type=Path, required=True)
    masksql_score.set_defaults(func=cmd_masksql_official_score)

    mask_only = subparsers.add_parser("mask-only", help="Run entity extraction + typed masking and score privacy metrics.")
    mask_only.add_argument("--data", type=Path, required=True)
    mask_only.add_argument("--output-results", type=Path, required=True)
    mask_only.add_argument("--output-metrics", type=Path, required=True)
    mask_only.add_argument("--base-url", default="http://127.0.0.1:8036/v1")
    mask_only.add_argument("--model-name", required=True)
    mask_only.add_argument("--api-key", default="EMPTY")
    mask_only.add_argument("--max-workers", type=int, default=4)
    mask_only.add_argument("--limit", type=int, default=0)
    mask_only.set_defaults(func=cmd_mask_only)

    baseline = subparsers.add_parser("baseline-sql", help="Run a Spider baseline with unified privacy and SQL metrics.")
    baseline.add_argument("--data", type=Path, required=True, help="Prepared JSONL data with attached MaskSQL annotations.")
    baseline.add_argument("--strategy", choices=["direct", "gold-masksql", "entity-mask"], required=True)
    baseline.add_argument("--output-dir", type=Path, required=True)
    baseline.add_argument("--sql-base-url", default="https://api.deepseek.com/v1")
    baseline.add_argument("--sql-model-name", default="deepseek-chat")
    baseline.add_argument("--sql-api-key", default="EMPTY")
    baseline.add_argument("--entity-base-url", default="http://127.0.0.1:8036/v1")
    baseline.add_argument("--entity-model-name", default=str(_RELEASE_ROOT / 'LlamaFactory/saves/entity_extract_v1/checkpoint-25'))
    baseline.add_argument("--entity-api-key", default="EMPTY")
    baseline.add_argument("--max-workers", type=int, default=4)
    baseline.add_argument("--limit", type=int, default=0)
    baseline.add_argument("--skip-exec-eval", action="store_true")
    baseline.add_argument("--gold-path", type=Path, default=Path("/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql"))
    baseline.add_argument("--db-root", type=Path, default=Path(str(_RELEASE_ROOT / 'privacy-agent/CHESS/spider/dev/dev_databases')))
    baseline.add_argument("--table-path", type=Path, default=Path("/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json"))
    baseline.set_defaults(func=cmd_baseline_sql)

    exec_score = subparsers.add_parser("exec-score", help="Compute lightweight Spider execution-match accuracy without NLTK.")
    exec_score.add_argument("--all-res", type=Path, default=None, help="Read pred_sql/gold_sql/db_id directly from all_res.json.")
    exec_score.add_argument("--gold-path", type=Path, default=None)
    exec_score.add_argument("--pred-path", type=Path, default=None)
    exec_score.add_argument("--db-root", type=Path, required=True)
    exec_score.add_argument("--output", type=Path, required=True)
    exec_score.set_defaults(func=cmd_exec_score)

    repair_quotes = subparsers.add_parser(
        "repair-sql-quotes",
        help="Repair quoted fully-qualified identifiers in an agent all_res.json.",
    )
    repair_quotes.add_argument("--all-res", type=Path, required=True)
    repair_quotes.add_argument("--output", type=Path, required=True)
    repair_quotes.set_defaults(func=cmd_repair_sql_quotes)

    privacy_repair = subparsers.add_parser(
        "privacy-repair",
        help="Apply local MaskSQL-annotation-aligned privacy repair to a PrivAgent all_res.json.",
    )
    privacy_repair.add_argument("--all-res", type=Path, required=True)
    privacy_repair.add_argument("--output", type=Path, required=True)
    privacy_repair.add_argument("--metrics-output", type=Path, required=True)
    privacy_repair.set_defaults(func=cmd_privacy_repair)

    privacy_repair_balanced = subparsers.add_parser(
        "privacy-repair-balanced",
        help="Apply conservative local privacy repair and record only actually repaired spans.",
    )
    privacy_repair_balanced.add_argument("--all-res", type=Path, required=True)
    privacy_repair_balanced.add_argument("--output", type=Path, required=True)
    privacy_repair_balanced.add_argument("--metrics-output", type=Path, required=True)
    privacy_repair_balanced.set_defaults(func=cmd_privacy_repair_balanced)

    enrich = subparsers.add_parser("enrich-schema", help="Replace row schemas with complete schemas rebuilt from SQLite databases.")
    enrich.add_argument("--data", type=Path, required=True)
    enrich.add_argument("--db-root", type=Path, required=True)
    enrich.add_argument("--output", type=Path, required=True)
    enrich.add_argument("--report", type=Path, default=None)
    enrich.set_defaults(func=cmd_enrich_schema)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
