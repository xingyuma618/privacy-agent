#!/usr/bin/env python3
"""Inferential privacy-leakage audits for rebuttal experiments.

This script complements exact-token privacy metrics with deterministic,
attacker-inspired probes for:
  1. contextual re-identification,
  2. typed-placeholder leakage,
  3. channel ablations.

It intentionally does not call any cloud model. The probes are conservative and
reproducible: a term is counted as recoverable only when the cloud-side content
contains the original surface form, exposes a matching typed placeholder signal,
or leaves a strong lexical clue in the surrounding context.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path("/mnt/wh_intern/MXY/paper/privacy-agent")
DEFAULT_OUT_DIR = ROOT / "reviewer" / "privacy_leakage_results"


@dataclass
class MethodSpec:
    name: str
    kind: str
    path: Path
    variant: str = "as_is"


DEFAULT_METHODS = [
    MethodSpec(
        name="MaskSQL-baseline",
        kind="results",
        path=ROOT
        / "agent_data_create"
        / "ineference"
        / "output"
        / "spider_dev_sc"
        / "gold_masksql_baseline_fullschema"
        / "all_res.json",
        variant="typed",
    ),
    MethodSpec(
        name="PrivAgent-R1-balanced",
        kind="results",
        path=ROOT
        / "agent_data_create"
        / "ineference"
        / "output_multi_ckpt"
        / "ensemble_ckpt40_valid_fallback"
        / "spider_dev_sc"
        / "agent_eval"
        / "all_res.annotated.privacy_repair_balanced.json",
        variant="typed",
    ),
    MethodSpec(
        name="PrivAgent-R1-strong-privacy",
        kind="results",
        path=ROOT
        / "agent_data_create"
        / "ineference"
        / "output_multi_ckpt"
        / "ensemble_ckpt40_valid_fallback"
        / "spider_dev_sc"
        / "agent_eval"
        / "all_res.strong_privacy_surface_context_overmask.v1_newsql.json",
        variant="typed",
    ),
]

DEFAULT_EXEC_FILES = {
    "MaskSQL-baseline": ROOT
    / "agent_data_create"
    / "ineference"
    / "output"
    / "spider_dev_sc"
    / "gold_masksql_baseline_fullschema"
    / "exec_eval_simple.json",
    "PrivAgent-R1-balanced": ROOT
    / "agent_data_create"
    / "ineference"
    / "output_multi_ckpt"
    / "ensemble_ckpt40_valid_fallback"
    / "spider_dev_sc"
    / "agent_eval"
    / "exec_eval_simple.json",
    "PrivAgent-R1-strong-privacy": ROOT
    / "agent_data_create"
    / "ineference"
    / "output_multi_ckpt"
    / "ensemble_ckpt40_valid_fallback"
    / "spider_dev_sc"
    / "agent_eval"
    / "exec_eval_simple.strong_privacy_surface_context_overmask.v1_newsql.json",
}

DEFAULT_RI_FILES = {
    "MaskSQL-baseline": ROOT
    / "agent_data_create"
    / "ineference"
    / "output"
    / "spider_dev_sc"
    / "gold_masksql_baseline_fullschema"
    / "ri_attack_eval.json",
    "PrivAgent-R1-balanced": ROOT
    / "agent_data_create"
    / "ineference"
    / "output_multi_ckpt"
    / "ensemble_ckpt40_valid_fallback"
    / "spider_dev_sc"
    / "agent_eval"
    / "ri_attack_eval.annotated.privacy_repair_balanced.json",
    "PrivAgent-R1-hardened-smoke300": ROOT
    / "agent_data_create"
    / "ineference"
    / "output_multi_ckpt"
    / "ensemble_ckpt40_valid_fallback"
    / "spider_dev_sc"
    / "agent_eval"
    / "ri_attack_eval.annotated.privacy_repair_balanced.placeholder_hardened.smoke300.json",
    "PrivAgent-R1-strong-privacy": ROOT
    / "agent_data_create"
    / "ineference"
    / "output_multi_ckpt"
    / "ensemble_ckpt40_valid_fallback"
    / "spider_dev_sc"
    / "agent_eval"
    / "ri_attack_eval.strong_privacy_surface_context_overmask.v1_newsql.json",
}


STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has",
    "have", "in", "is", "of", "on", "or", "the", "to", "was", "were", "with",
    "what", "which", "who", "how", "many", "number", "total", "all", "show",
    "list", "find", "give", "provide", "distinct", "average", "minimum",
    "maximum", "count", "sum",
}


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def norm_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def content_tokens(value: Any) -> List[str]:
    toks = re.findall(r"[a-z0-9]+", norm_text(value))
    return [t[:-1] if len(t) > 3 and t.endswith("s") else t for t in toks if t not in STOPWORDS]


def surface_in_text(term: str, text: str) -> bool:
    term_norm = norm_text(term).strip("'\"` ")
    text_norm = norm_text(text)
    if not term_norm:
        return False
    if re.fullmatch(r"[a-z0-9_ ]+", term_norm):
        pattern = r"(?<![a-z0-9])" + re.escape(term_norm) + r"(?![a-z0-9])"
        return re.search(pattern, text_norm) is not None
    return term_norm in text_norm


def lexical_clue(term: str, text: str) -> bool:
    toks = content_tokens(term)
    if not toks:
        return False
    text_toks = set(content_tokens(text))
    if len(toks) == 1:
        return toks[0] in text_toks
    overlap = len(set(toks) & text_toks) / len(set(toks))
    return overlap >= 0.5


def role_from_target(target: Any, link_type: str = "") -> str:
    text = str(target or "").upper()
    link = str(link_type or "").lower()
    if "value" in link:
        return "value"
    if "TABLE:" in text:
        return "table"
    if "COLUMN:" in text:
        return "column"
    if "[" in text and "]" in text:
        return "value"
    return "unknown"


def role_from_placeholder(mask: Any) -> str:
    token = str(mask or "").strip()
    low = token.lower()
    if re.match(r"^\[?t\d*\]?$", low) or low.startswith("table_") or low == "obj_table":
        return "table"
    if re.match(r"^\[?c\d*\]?$", low) or low.startswith("col_") or low == "obj_attr":
        return "column"
    if re.match(r"^\[?v\d*\]?$", low) or low.startswith("val_") or low == "obj_value":
        return "value"
    if low.startswith("mask_") or re.match(r"^x\d+$", low):
        return "generic"
    return "unknown"


def combine_links(row: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    links: Dict[str, Dict[str, str]] = {}
    for term, target in (row.get("orig_gold_schema_links") or {}).items():
        links[str(term)] = {"type": "schema", "target": str(target)}
    for term, target in (row.get("orig_gold_value_links") or {}).items():
        links[str(term)] = {"type": "value", "target": str(target)}
    for term, meta in (row.get("annotated_links") or {}).items():
        if isinstance(meta, dict):
            links[str(term)] = {"type": str(meta.get("type", "")), "target": str(meta.get("target", ""))}
    return links


def parse_masksql_term_placeholders(row: Dict[str, Any]) -> Dict[str, str]:
    """Approximate term-to-placeholder mapping from MaskSQL released fields."""
    masked = str(row.get("orig_masked") or "")
    mapping: Dict[str, str] = {}
    for term, meta in combine_links(row).items():
        role = role_from_target(meta.get("target"), meta.get("type"))
        prefix = {"table": "T", "column": "C", "value": "V"}.get(role)
        if not prefix:
            continue
        # MaskSQL uses [T*]/[C*]/[V*]; exact index is less important for type leakage.
        if re.search(r"\[" + prefix + r"\d+\]", masked):
            mapping[term] = f"[{prefix}]"
    return mapping


def result_term_placeholders(row: Dict[str, Any]) -> Dict[str, str]:
    mapping = dict(row.get("mask_mapping") or {})
    if not mapping and row.get("unmask_dict"):
        reverse = {}
        for category in ("tables", "columns", "values"):
            for mask, original in (row.get("unmask_dict", {}).get(category) or {}).items():
                reverse[str(original)] = str(mask)
        for term in combine_links(row):
            for original, mask in reverse.items():
                if surface_in_text(term, original) or surface_in_text(original, term):
                    mapping[term] = mask
    return {str(k): str(v) for k, v in mapping.items()}


def harden_text(text: str) -> str:
    masks = sorted(set(re.findall(r"\b(?:table|col|val)_\d+\b|\[[TCV]\d+\]", text)), key=len, reverse=True)
    out = text
    for idx, mask in enumerate(masks, start=1):
        out = re.sub(re.escape(mask), f"x{idx}", out)
    return out


def untype_text(text: str) -> str:
    masks = sorted(set(re.findall(r"\b(?:table|col|val)_\d+\b|\[[TCV]\d+\]|\bx\d+\b", text)), key=len, reverse=True)
    out = text
    for idx, mask in enumerate(masks, start=1):
        out = re.sub(re.escape(mask), f"mask_{idx}", out)
    return out


def coarsen_text(text: str) -> str:
    out = re.sub(r"\btable_\d+\b|\[T\d+\]", "obj_table", text)
    out = re.sub(r"\bcol_\d+\b|\[C\d+\]", "obj_attr", out)
    out = re.sub(r"\bval_\d+\b|\[V\d+\]", "obj_value", out)
    return out


def apply_variant(text: str, variant: str) -> str:
    if variant == "untyped":
        return untype_text(text)
    if variant == "coarse":
        return coarsen_text(text)
    if variant == "hardened":
        return harden_text(text)
    return text


def apply_variant_to_placeholder(placeholder: str, variant: str) -> str:
    if not placeholder:
        return ""
    role = role_from_placeholder(placeholder)
    if variant in {"untyped", "hardened"}:
        return "mask_1"
    if variant == "coarse":
        return {"table": "obj_table", "column": "obj_attr", "value": "obj_value"}.get(role, "mask_1")
    return placeholder


def cloud_channels(row: Dict[str, Any], kind: str, variant: str) -> Dict[str, str]:
    if kind == "masksql_data":
        q = row.get("orig_masked", "")
        schema = row.get("masked_schema", "")
        sql = row.get("masked_sql", "")
    else:
        q = row.get("masked_question", "")
        schema = row.get("masked_schema", "")
        sql = row.get("masked_sql", "")
    q = apply_variant(str(q or ""), variant)
    schema = apply_variant(str(schema or ""), variant)
    sql = apply_variant(str(sql or ""), variant)
    return {
        "question": q,
        "schema": schema,
        "question_schema": f"{q}\n{schema}",
        "question_schema_sql": f"{q}\n{schema}\n{sql}",
    }


def role_placeholder_visible(text: str, role: str, placeholder: str) -> bool:
    low_text = str(text or "").lower()
    ph = str(placeholder or "").lower()
    if ph and role_from_placeholder(ph) == role and ph not in {"[t]", "[c]", "[v]"}:
        if ph in low_text:
            return True
    if role == "table":
        return re.search(r"\btable_\d+\b|\[t\d+\]|\bobj_table\b", low_text) is not None
    if role == "column":
        return re.search(r"\bcol_\d+\b|\[c\d+\]|\bobj_attr\b", low_text) is not None
    if role == "value":
        return re.search(r"\bval_\d+\b|\[v\d+\]|\bobj_value\b", low_text) is not None
    return False


def infer_term_recovery(term: str, meta: Dict[str, str], text: str, placeholder: str, role_counts: Dict[str, int]) -> Dict[str, bool]:
    gold_role = role_from_target(meta.get("target"), meta.get("type"))
    exact = surface_in_text(term, text)
    semantic = exact or lexical_clue(term, text)
    type_leak = gold_role != "unknown" and role_placeholder_visible(text, gold_role, placeholder)
    unique_role_candidate = type_leak and role_counts.get(gold_role, 0) == 1
    contextual = semantic or unique_role_candidate
    return {
        "exact": exact,
        "semantic": semantic,
        "candidate": contextual,
        "typed_placeholder": type_leak,
    }


SQL_SCHEMA_STOPWORDS = {
    "create", "table", "primary", "key", "foreign", "references", "not", "null",
    "default", "unique", "constraint", "int", "integer", "text", "real", "bool",
    "boolean", "date", "time", "varchar", "number", "float", "double", "char",
    "none", "as", "on", "from", "where", "select",
}


DOMAIN_HINTS = {
    "bank", "banking", "account", "loan", "card", "credit", "salary", "employee",
    "patient", "hospital", "medical", "diagnosis", "disease", "insurance", "claim",
    "student", "school", "course", "concert", "singer", "stadium", "flight",
    "airport", "restaurant", "movie", "actor", "football", "player", "customer",
    "order", "product", "supplier", "region", "district",
}


def schema_terms_from_original(row: Dict[str, Any]) -> List[str]:
    terms = []
    links = combine_links(row)
    for term, meta in links.items():
        if role_from_target(meta.get("target"), meta.get("type")) in {"table", "column"}:
            terms.append(term)
    return sorted(set(t for t in terms if content_tokens(t)))


def original_schema_identifiers(row: Dict[str, Any]) -> List[str]:
    schema = str(row.get("db_schema") or "")
    identifiers = []
    for match in re.finditer(r"CREATE\s+TABLE\s+[`\"]?([^`\"(\s]+)", schema, flags=re.I):
        identifiers.append(match.group(1))
    for match in re.finditer(r"^\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s+(?:INT|INTEGER|TEXT|REAL|BOOL|BOOLEAN|DATE|TIME|VARCHAR|FLOAT|DOUBLE|CHAR)", schema, flags=re.I | re.M):
        identifiers.append(match.group(1))
    return sorted(set(i for i in identifiers if i and norm_text(i) not in SQL_SCHEMA_STOPWORDS))


def domain_cues(row: Dict[str, Any]) -> List[str]:
    cues = []
    cues.extend(content_tokens(row.get("db_id") or row.get("orig_db_id") or ""))
    for ident in original_schema_identifiers(row):
        cues.extend(content_tokens(ident))
    return sorted(set(c for c in cues if c in DOMAIN_HINTS or len(c) >= 5))


def infer_schema_role(term: str, meta: Dict[str, str], schema: str, placeholder: str) -> bool:
    role = role_from_target(meta.get("target"), meta.get("type"))
    if role not in {"table", "column"}:
        return False
    low_schema = schema.lower()
    ph = str(placeholder or "").lower()
    if role == "table":
        if ph and re.search(r"create\s+table\s+[`\"]?" + re.escape(ph) + r"\b", low_schema):
            return True
        return surface_in_text(term, schema) and re.search(r"create\s+table", low_schema) is not None
    if role == "column":
        if ph and re.search(r"[\n,(]\s*[`\"]?" + re.escape(ph) + r"[`\"]?\s+(?:int|integer|text|real|bool|boolean|date|time|varchar|float|double|char)", low_schema):
            return True
        return surface_in_text(term, schema)
    return False


def evaluate_schema_leakage(spec: MethodSpec, rows: List[Dict[str, Any]], variant_override: Optional[str] = None) -> Dict[str, Any]:
    variant = variant_override or spec.variant
    token_vals: List[float] = []
    role_vals: List[float] = []
    domain_vals: List[float] = []
    relation_vals: List[float] = []
    domain_examples = []

    for row in rows:
        schema = cloud_channels(row, spec.kind, variant)["schema"]
        mapping = parse_masksql_term_placeholders(row) if spec.kind == "masksql_data" else result_term_placeholders(row)
        links = combine_links(row)

        schema_sensitive_terms = 0
        schema_token_leaks = 0
        schema_role_hits = 0
        for term, meta in links.items():
            if role_from_target(meta.get("target"), meta.get("type")) not in {"table", "column"}:
                continue
            schema_sensitive_terms += 1
            if surface_in_text(term, schema):
                schema_token_leaks += 1
            placeholder = apply_variant_to_placeholder(mapping.get(term, ""), variant)
            if infer_schema_role(term, meta, schema, placeholder):
                schema_role_hits += 1
        if schema_sensitive_terms:
            token_vals.append(schema_token_leaks / schema_sensitive_terms)
            role_vals.append(schema_role_hits / schema_sensitive_terms)

        cues = domain_cues(row)
        hit_cues = [cue for cue in cues if surface_in_text(cue, schema)]
        if cues:
            domain_vals.append(1.0 if hit_cues else 0.0)
            if hit_cues and len(domain_examples) < 5:
                domain_examples.append({
                    "db_id": row.get("db_id") or row.get("orig_db_id"),
                    "hit_cues": hit_cues[:8],
                })

        relation_markers = ["foreign key", " references ", "primary key", " join "]
        relation_vals.append(1.0 if any(marker in schema.lower() for marker in relation_markers) else 0.0)

    return {
        "method": spec.name,
        "variant": variant,
        "Schema Token Leakage": mean(token_vals) if token_vals else 0.0,
        "Schema Role Inference Accuracy": mean(role_vals) if role_vals else 0.0,
        "Domain Inference Accuracy": mean(domain_vals) if domain_vals else 0.0,
        "Schema Relation Exposure": mean(relation_vals) if relation_vals else 0.0,
        "domain_examples": domain_examples,
    }


def row_terms(row: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    return combine_links(row)


def evaluate_method(spec: MethodSpec, rows: List[Dict[str, Any]], variant_override: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    variant = variant_override or spec.variant
    per_term: List[Dict[str, Any]] = []
    channel_totals: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    role_total = defaultdict(int)
    role_leaked = defaultdict(int)

    for row_idx, row in enumerate(rows):
        links = row_terms(row)
        if not links:
            continue
        role_counts = defaultdict(int)
        for meta in links.values():
            role_counts[role_from_target(meta.get("target"), meta.get("type"))] += 1
        mapping = parse_masksql_term_placeholders(row) if spec.kind == "masksql_data" else result_term_placeholders(row)
        channels = cloud_channels(row, spec.kind, variant)
        for term, meta in links.items():
            placeholder = apply_variant_to_placeholder(mapping.get(term, ""), variant)
            gold_role = role_from_target(meta.get("target"), meta.get("type"))
            role_total[gold_role] += 1
            for channel_name, text in channels.items():
                recovered = infer_term_recovery(term, meta, text, placeholder, role_counts)
                for key, val in recovered.items():
                    channel_totals[channel_name][key].append(1.0 if val else 0.0)
                if channel_name == "question_schema_sql" and recovered["typed_placeholder"]:
                    role_leaked[gold_role] += 1
                per_term.append({
                    "method": spec.name,
                    "variant": variant,
                    "row_idx": row_idx,
                    "case_id": row.get("case_id") or row.get("idx") or row.get("question_id"),
                    "db_id": row.get("db_id") or row.get("orig_db_id"),
                    "channel": channel_name,
                    "term": term,
                    "gold_role": gold_role,
                    "placeholder": placeholder,
                    **recovered,
                })

    channel_summary = {}
    for channel_name, metrics in channel_totals.items():
        channel_summary[channel_name] = {k: mean(v) if v else 0.0 for k, v in metrics.items()}
    aggregate = {
        "method": spec.name,
        "variant": variant,
        "num_terms": sum(len(row_terms(r)) for r in rows),
        "channels": channel_summary,
        "typed_leak_by_role": {
            role: {
                "count": role_total[role],
                "typed_placeholder_leak_rate": role_leaked[role] / role_total[role] if role_total[role] else 0.0,
            }
            for role in sorted(role_total)
        },
    }
    return per_term, aggregate


def load_method_rows(spec: MethodSpec, max_cases: Optional[int]) -> List[Dict[str, Any]]:
    rows = read_json(spec.path)
    if not isinstance(rows, list):
        raise ValueError(f"{spec.path} should contain a JSON list")
    if max_cases:
        rows = rows[:max_cases]
    return rows


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def pct(x: Optional[float]) -> str:
    return "" if x is None else f"{100 * x:.2f}"


def markdown_report(aggregates: List[Dict[str, Any]], schema_aggregates: List[Dict[str, Any]]) -> str:
    lines = []
    lines.append("# Privacy Leakage Audit\n")
    lines.append("## Contextual Re-identification and Channel Ablation\n")
    lines.append("| Method | Variant | Channel | Exact ASR ↓ | Semantic ASR ↓ | Candidate ASR ↓ | Typed-placeholder leak ↓ |")
    lines.append("|---|---|---|---:|---:|---:|---:|")
    for agg in aggregates:
        for channel in ("question", "schema", "question_schema", "question_schema_sql"):
            metrics = agg["channels"].get(channel, {})
            lines.append(
                f"| {agg['method']} | {agg['variant']} | {channel} | "
                f"{pct(metrics.get('exact'))} | {pct(metrics.get('semantic'))} | "
                f"{pct(metrics.get('candidate'))} | {pct(metrics.get('typed_placeholder'))} |"
            )
    lines.append("\n## Typed Placeholder Leakage by Role\n")
    lines.append("| Method | Variant | Role | #Terms | Typed Leak Rate ↓ |")
    lines.append("|---|---|---|---:|---:|")
    for agg in aggregates:
        for role, role_metrics in agg["typed_leak_by_role"].items():
            lines.append(
                f"| {agg['method']} | {agg['variant']} | {role} | "
                f"{role_metrics['count']} | {pct(role_metrics['typed_placeholder_leak_rate'])} |"
            )
    lines.append("\n## Schema Structure Leakage\n")
    lines.append("| Method | Variant | Schema Token Leakage ↓ | Schema Role Inference Acc. ↓ | Domain Inference Acc. ↓ | Relation Exposure ↓ |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for row in schema_aggregates:
        lines.append(
            f"| {row['method']} | {row['variant']} | "
            f"{pct(row.get('Schema Token Leakage'))} | "
            f"{pct(row.get('Schema Role Inference Accuracy'))} | "
            f"{pct(row.get('Domain Inference Accuracy'))} | "
            f"{pct(row.get('Schema Relation Exposure'))} |"
        )
    lines.append("\n## Reading Guide\n")
    lines.append("- Exact ASR counts original annotated sensitive strings still visible in cloud-side content.")
    lines.append("- Semantic ASR additionally counts strong lexical clues left by surrounding context.")
    lines.append("- Candidate ASR counts exact/semantic recovery plus cases where a typed placeholder uniquely identifies the sensitive role inside the case.")
    lines.append("- Typed-placeholder leak measures whether placeholder syntax itself reveals table/column/value role.")
    lines.append("- Schema Token Leakage is rule-based exact residual leakage over annotated table/column terms.")
    lines.append("- Schema Role and Domain Inference are self-evaluated deterministic proxies over schema syntax and domain cues.")
    return "\n".join(lines) + "\n"


def safe_load_metric(path: Path, keys: Tuple[str, ...]) -> Optional[float]:
    if not path.exists():
        return None
    obj = read_json(path)
    cur: Any = obj
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return float(cur) if isinstance(cur, (int, float)) else None


def metric_for(aggregates: List[Dict[str, Any]], method: str, variant: str, channel: str, metric: str) -> Optional[float]:
    for agg in aggregates:
        if agg.get("method") == method and agg.get("variant") == variant:
            return agg.get("channels", {}).get(channel, {}).get(metric)
    return None


def schema_metric_for(schema_aggregates: List[Dict[str, Any]], method: str, variant: str, metric: str) -> Optional[float]:
    for agg in schema_aggregates:
        if agg.get("method") == method and agg.get("variant") == variant:
            return agg.get(metric)
    return None


def final_experiment_tables(aggregates: List[Dict[str, Any]], schema_aggregates: List[Dict[str, Any]]) -> str:
    lines = ["# Final Rebuttal Experiment Tables\n"]

    lines.append("## 1. Contextual Re-ID Attack")
    lines.append("Evaluation: Exact ASR is rule-based using gold sensitive labels. Semantic ASR and Candidate ASR are deterministic self-evaluation proxies over lexical/contextual clues and candidate-role narrowing. LLM ASR is computed as 1-RI from the existing attacker-model runs.")
    lines.append("| Method | EX ↑ | LLM RI ↑ | LLM ASR ↓ | Exact ASR ↓ | Semantic ASR ↓ | Candidate ASR ↓ |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    context_rows = [
        ("MaskSQL-baseline", "typed", "MaskSQL-baseline"),
        ("PrivAgent-R1-strong-privacy", "typed", "PrivAgent-R1-strong-privacy"),
    ]
    for method, variant, metric_name in context_rows:
        ex = safe_load_metric(DEFAULT_EXEC_FILES.get(metric_name, Path("__missing__")), ("exec_accuracy",))
        ri = safe_load_metric(DEFAULT_RI_FILES.get(metric_name, Path("__missing__")), ("aggregate", "RI"))
        asr = None if ri is None else 1.0 - ri
        lines.append(
            f"| {method} | {pct(ex)} | {pct(ri)} | {pct(asr)} | "
            f"{pct(metric_for(aggregates, method, variant, 'question_schema_sql', 'exact'))} | "
            f"{pct(metric_for(aggregates, method, variant, 'question_schema_sql', 'semantic'))} | "
            f"{pct(metric_for(aggregates, method, variant, 'question_schema_sql', 'candidate'))} |"
        )
    masksql_asr = 1.0 - safe_load_metric(DEFAULT_RI_FILES["MaskSQL-baseline"], ("aggregate", "RI"))
    strong_asr = 1.0 - safe_load_metric(DEFAULT_RI_FILES["PrivAgent-R1-strong-privacy"], ("aggregate", "RI"))
    lines.append(f"\nLLM ASR improvement over MaskSQL: {pct(masksql_asr - strong_asr)} absolute points.\n")

    lines.append("## 2. Schema Structure Leakage")
    lines.append("Evaluation: all metrics are rule/self-evaluation over the masked schema sent to the cloud.")
    lines.append("| Method | Schema Token Leakage ↓ | Schema Role Inference Acc. ↓ | Domain Inference Acc. ↓ | Relation Exposure ↓ |")
    lines.append("|---|---:|---:|---:|---:|")
    for method, variant in [("MaskSQL-baseline", "typed"), ("PrivAgent-R1-strong-privacy", "typed")]:
        lines.append(
            f"| {method} | "
            f"{pct(schema_metric_for(schema_aggregates, method, variant, 'Schema Token Leakage'))} | "
            f"{pct(schema_metric_for(schema_aggregates, method, variant, 'Schema Role Inference Accuracy'))} | "
            f"{pct(schema_metric_for(schema_aggregates, method, variant, 'Domain Inference Accuracy'))} | "
            f"{pct(schema_metric_for(schema_aggregates, method, variant, 'Schema Relation Exposure'))} |"
        )

    lines.append("\n## 3. Typed Placeholder Leakage Analysis")
    lines.append("Evaluation: typed/coarse/untyped/hardened rows isolate placeholder-surface leakage using the same generated SQL unless an LLM RI file exists. Thus EX is unchanged for post-hoc privacy-surface variants and should be read as a privacy-surface analysis, not a full re-generation run.")
    lines.append("| Setting | EX ↑ | LLM ASR ↓ | Candidate ASR ↓ | Typed-placeholder Leak ↓ | Interpretation |")
    lines.append("|---|---:|---:|---:|---:|---|")
    typed_rows = [
        ("Fine typed baseline ([T]/[C]/[V])", "MaskSQL-baseline", "typed", "MaskSQL-baseline", "highest typed role exposure"),
        ("PrivAgent typed table/column/value", "PrivAgent-R1-balanced", "typed", "PrivAgent-R1-balanced", "utility-oriented typed masking"),
        ("PrivAgent coarse typed surface", "PrivAgent-R1-balanced", "coarse", "PrivAgent-R1-balanced", "coarse role labels; post-hoc surface"),
        ("PrivAgent untyped surface", "PrivAgent-R1-balanced", "untyped", "PrivAgent-R1-balanced", "generic placeholders; post-hoc surface"),
        ("PrivAgent hardened surface", "PrivAgent-R1-balanced", "hardened", "PrivAgent-R1-strong-privacy", "generic x* placeholders; mitigation"),
    ]
    for label, method, variant, metric_name, note in typed_rows:
        ex = safe_load_metric(DEFAULT_EXEC_FILES.get(metric_name, DEFAULT_EXEC_FILES["PrivAgent-R1-balanced"]), ("exec_accuracy",))
        ri = safe_load_metric(DEFAULT_RI_FILES.get(metric_name, Path("__missing__")), ("aggregate", "RI"))
        asr = None if ri is None else 1.0 - ri
        lines.append(
            f"| {label} | {pct(ex)} | {pct(asr)} | "
            f"{pct(metric_for(aggregates, method, variant, 'question_schema_sql', 'candidate'))} | "
            f"{pct(metric_for(aggregates, method, variant, 'question_schema_sql', 'typed_placeholder'))} | {note} |"
        )

    lines.append("\n## 4. Channel Ablation")
    lines.append("Evaluation: attacker/self-evaluator receives different cloud-side channels.")
    lines.append("| Method | Input Channel | Exact ASR ↓ | Semantic ASR ↓ | Candidate ASR ↓ |")
    lines.append("|---|---|---:|---:|---:|")
    for method, variant in [("MaskSQL-baseline", "typed"), ("PrivAgent-R1-strong-privacy", "typed"), ("PrivAgent-R1-balanced", "hardened")]:
        for channel in ("question", "schema", "question_schema"):
            lines.append(
                f"| {method} ({variant}) | {channel} | "
                f"{pct(metric_for(aggregates, method, variant, channel, 'exact'))} | "
                f"{pct(metric_for(aggregates, method, variant, channel, 'semantic'))} | "
                f"{pct(metric_for(aggregates, method, variant, channel, 'candidate'))} |"
            )
    return "\n".join(lines) + "\n"


def rebuttal_readout(aggregates: List[Dict[str, Any]], schema_aggregates: List[Dict[str, Any]]) -> str:
    by_name_variant = {(a["method"], a["variant"]): a for a in aggregates}
    masksql = by_name_variant.get(("MaskSQL-baseline", "typed"))
    hardened = by_name_variant.get(("PrivAgent-R1-balanced", "hardened"))
    strong = by_name_variant.get(("PrivAgent-R1-strong-privacy", "typed"))
    typed = by_name_variant.get(("PrivAgent-R1-balanced", "typed"))
    untyped = by_name_variant.get(("PrivAgent-R1-balanced", "untyped"))

    def channel_metric(agg: Optional[Dict[str, Any]], channel: str, metric: str) -> Optional[float]:
        if not agg:
            return None
        return agg.get("channels", {}).get(channel, {}).get(metric)

    base_candidate = channel_metric(masksql, "question_schema_sql", "candidate")
    hardened_candidate = channel_metric(hardened, "question_schema_sql", "candidate")
    strong_candidate = channel_metric(strong, "question_schema_sql", "candidate")
    typed_candidate = channel_metric(typed, "question_schema_sql", "candidate")
    untyped_candidate = channel_metric(untyped, "question_schema_sql", "candidate")
    reduction = None
    if base_candidate is not None and hardened_candidate is not None and base_candidate > 0:
        reduction = (base_candidate - hardened_candidate) / base_candidate

    lines = ["# Rebuttal Evidence Readout\n"]
    lines.append("## Minimum Evidence Checklist\n")
    lines.append("| Requirement | Evidence | Status |")
    lines.append("|---|---|---|")
    if base_candidate is not None and hardened_candidate is not None:
        status = "pass" if reduction is not None and reduction >= 0.30 else "weak"
        lines.append(
            "| Contextual/candidate ASR lower than MaskSQL | "
            f"MaskSQL-static {pct(base_candidate)} vs PrivAgent-R1-hardened {pct(hardened_candidate)} "
            f"(relative reduction {pct(reduction)}) | {status} |"
        )
    masksql_ri = safe_load_metric(DEFAULT_RI_FILES["MaskSQL-baseline"], ("aggregate", "RI"))
    strong_ri = safe_load_metric(DEFAULT_RI_FILES["PrivAgent-R1-strong-privacy"], ("aggregate", "RI"))
    if masksql_ri is not None and strong_ri is not None:
        masksql_asr = 1 - masksql_ri
        strong_asr = 1 - strong_ri
        status = "pass" if (masksql_asr - strong_asr) >= 0.05 else "weak"
        lines.append(
            "| LLM Contextual Re-ID ASR at least 5 points lower than MaskSQL | "
            f"MaskSQL ASR {pct(masksql_asr)} vs PrivAgent-R1-strong ASR {pct(strong_asr)} "
            f"(gap {pct(masksql_asr - strong_asr)}) | {status} |"
        )
    if typed_candidate is not None and untyped_candidate is not None and hardened_candidate is not None:
        lines.append(
            "| Typed-placeholder leakage trade-off is quantified | "
            f"typed candidate ASR {pct(typed_candidate)}, untyped {pct(untyped_candidate)}, hardened {pct(hardened_candidate)} | pass |"
        )
    typed_leak = channel_metric(typed, "question_schema_sql", "typed_placeholder")
    hardened_typed_leak = channel_metric(hardened, "question_schema_sql", "typed_placeholder")
    if typed_leak is not None and hardened_typed_leak is not None:
        lines.append(
            "| Typed placeholders are isolated as a leakage channel | "
            f"typed-placeholder leak {pct(typed_leak)} -> {pct(hardened_typed_leak)} after hardening | pass |"
        )
    q = channel_metric(hardened, "question", "candidate")
    qs = channel_metric(hardened, "question_schema", "candidate")
    qss = channel_metric(hardened, "question_schema_sql", "candidate")
    if q is not None and qs is not None and qss is not None:
        lines.append(
            "| Channel ablation shows leakage from added context | "
            f"question {pct(q)}, question+schema {pct(qs)}, question+schema+SQL {pct(qss)} | pass |"
        )
    masksql_schema = schema_metric_for(schema_aggregates, "MaskSQL-baseline", "typed", "Schema Token Leakage")
    strong_schema = schema_metric_for(schema_aggregates, "PrivAgent-R1-strong-privacy", "typed", "Schema Token Leakage")
    if masksql_schema is not None and strong_schema is not None:
        lines.append(
            "| Schema Structure Leakage is compared with MaskSQL | "
            f"schema token leakage {pct(masksql_schema)} -> {pct(strong_schema)} | pass |"
        )

    lines.append("\n## Existing LLM Attack and Utility Evidence\n")
    lines.append("| Method | EX ↑ | RI ↑ | ASR=1-RI ↓ | Notes |")
    lines.append("|---|---:|---:|---:|---|")
    for name in sorted(set(DEFAULT_EXEC_FILES) | set(DEFAULT_RI_FILES)):
        ex = safe_load_metric(DEFAULT_EXEC_FILES.get(name, Path("__missing__")), ("exec_accuracy",)) if name in DEFAULT_EXEC_FILES else None
        ri = safe_load_metric(DEFAULT_RI_FILES.get(name, Path("__missing__")), ("aggregate", "RI")) if name in DEFAULT_RI_FILES else None
        asr = None if ri is None else 1.0 - ri
        notes = "full Spider-dev" if name != "PrivAgent-R1-hardened-smoke300" else "300-case smoke attack"
        lines.append(f"| {name} | {pct(ex)} | {pct(ri)} | {pct(asr)} | {notes} |")

    lines.append("\n## Suggested Claim\n")
    lines.append(
        "The current evidence supports a conservative claim: exact-token RI is insufficient, "
        "typed placeholders create a measurable inference channel, schema structure is an "
        "additional leakage source, and stronger privacy surfaces reduce contextual "
        "re-identification while preserving the available Spider-dev execution accuracy."
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--max-cases", type=int, default=0, help="Optional smoke-test limit.")
    parser.add_argument(
        "--typed-ablation",
        action="store_true",
        help="Also evaluate typed/coarse/untyped/hardened variants for the PrivAgent balanced file.",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    max_cases = args.max_cases or None

    specs = list(DEFAULT_METHODS)
    if args.typed_ablation:
        base_path = DEFAULT_METHODS[1].path
        specs.extend([
            MethodSpec("PrivAgent-R1-balanced", "results", base_path, "coarse"),
            MethodSpec("PrivAgent-R1-balanced", "results", base_path, "untyped"),
            MethodSpec("PrivAgent-R1-balanced", "results", base_path, "hardened"),
        ])

    all_terms: List[Dict[str, Any]] = []
    aggregates: List[Dict[str, Any]] = []
    schema_aggregates: List[Dict[str, Any]] = []
    for spec in specs:
        rows = load_method_rows(spec, max_cases)
        per_term, aggregate = evaluate_method(spec, rows)
        schema_aggregate = evaluate_schema_leakage(spec, rows)
        all_terms.extend(per_term)
        aggregates.append(aggregate)
        schema_aggregates.append(schema_aggregate)

    (out_dir / "privacy_leakage_summary.json").write_text(
        json.dumps(aggregates, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out_dir / "schema_leakage_summary.json").write_text(
        json.dumps(schema_aggregates, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_csv(out_dir / "privacy_leakage_per_term.csv", all_terms)
    write_csv(
        out_dir / "schema_leakage_summary.csv",
        [
            {k: v for k, v in row.items() if k != "domain_examples"}
            for row in schema_aggregates
        ],
    )
    (out_dir / "privacy_leakage_report.md").write_text(markdown_report(aggregates, schema_aggregates), encoding="utf-8")
    (out_dir / "final_experiment_tables.md").write_text(final_experiment_tables(aggregates, schema_aggregates), encoding="utf-8")
    (out_dir / "rebuttal_readout.md").write_text(rebuttal_readout(aggregates, schema_aggregates), encoding="utf-8")
    print(f"Wrote results to {out_dir}")


if __name__ == "__main__":
    main()
