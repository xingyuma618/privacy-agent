#!/usr/bin/env python3
"""Audit direct sensitive-entity exposure over every cloud SQL call.

The audit uses human gold annotations only for evaluation.  They are never
available to, or used by, the inference-time privacy gate.

Old runs store cloud-call arguments in per-case text logs.  New runs may also
store them directly in ``all_res.json`` under ``outbound_trace``.  This script
supports both formats so the original paper run can be audited without rerun.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


SQL_GENERATE_MARKER = "[Action - Tool Call]: sql_generate"
ARGUMENT_MARKER = "[Action - Arguments]:"


def normalize_surface(value: Any) -> str:
    text = str(value or "").strip().strip("'\"` ").lower()
    return re.sub(r"\s+", " ", text)


def surface_in_text(term: str, text: str) -> bool:
    """Use the same normalized exact-surface rule as the paper evaluator."""
    term_norm = normalize_surface(term)
    text_norm = normalize_surface(text)
    if not term_norm:
        return False
    if re.fullmatch(r"[a-z0-9_]+", term_norm):
        pattern = r"(?<![a-z0-9_])" + re.escape(term_norm) + r"(?![a-z0-9_])"
        return re.search(pattern, text_norm) is not None
    return term_norm in text_norm


def parse_calls_from_log(log_path: Path) -> List[Dict[str, Any]]:
    text = log_path.read_text(encoding="utf-8", errors="replace")
    decoder = json.JSONDecoder()
    calls: List[Dict[str, Any]] = []
    cursor = 0
    while True:
        call_pos = text.find(SQL_GENERATE_MARKER, cursor)
        if call_pos < 0:
            break
        arg_pos = text.find(ARGUMENT_MARKER, call_pos + len(SQL_GENERATE_MARKER))
        if arg_pos < 0:
            raise ValueError(f"Missing argument block after sql_generate in {log_path}")
        raw = text[arg_pos + len(ARGUMENT_MARKER) :].lstrip()
        args, consumed = decoder.raw_decode(raw)
        if not isinstance(args, dict):
            raise ValueError(f"sql_generate arguments are not an object in {log_path}")
        calls.append(
            {
                "attempt_index": len(calls) + 1,
                "allowed": True,
                "actual_cloud_call": True,
                "masked_query": str(args.get("masked_query", "")),
                "masked_schema_str": str(args.get("masked_schema_str", "")),
                "source": "case_log",
            }
        )
        cursor = arg_pos + len(ARGUMENT_MARKER) + consumed
    return calls


def calls_for_row(row: Dict[str, Any], case_logs: Path) -> List[Dict[str, Any]]:
    trace = row.get("outbound_trace")
    if isinstance(trace, list):
        # Only content that actually crossed the trust boundary counts as sent.
        return [
            call
            for call in trace
            if isinstance(call, dict)
            and call.get("allowed", True)
            and call.get("actual_cloud_call", True)
        ]
    case_id = str(row.get("case_id", row.get("question_id", "")))
    log_path = case_logs / f"case_{case_id}.txt"
    if not log_path.exists():
        return []
    return parse_calls_from_log(log_path)


def annotated_terms(row: Dict[str, Any]) -> List[str]:
    links = row.get("annotated_links") or {}
    if isinstance(links, dict):
        return [str(term) for term in links]
    return []


def audit(all_res_path: Path, case_logs: Path) -> Dict[str, Any]:
    rows = json.loads(all_res_path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError("all_res.json must contain a list")

    per_case: List[Dict[str, Any]] = []
    call_count_distribution: Counter[int] = Counter()
    total_cloud_calls = 0
    leaky_cloud_calls = 0
    total_sensitive_entities = 0
    exposed_entity_occurrences = 0

    for row in rows:
        case_id = str(row.get("case_id", row.get("question_id", "")))
        terms = annotated_terms(row)
        calls = calls_for_row(row, case_logs)
        call_count_distribution[len(calls)] += 1
        total_cloud_calls += len(calls)
        total_sensitive_entities += len(terms)

        call_reports = []
        exposed_terms_over_trajectory = set()
        query_exposed_terms_over_trajectory = set()
        schema_exposed_terms_over_trajectory = set()
        for call_index, call in enumerate(calls, start=1):
            query = str(call.get("masked_query", ""))
            schema = str(call.get("masked_schema_str", call.get("masked_schema", "")))
            query_hits = [term for term in terms if surface_in_text(term, query)]
            schema_hits = [term for term in terms if surface_in_text(term, schema)]
            hits = sorted(set(query_hits + schema_hits), key=normalize_surface)
            if hits:
                leaky_cloud_calls += 1
                exposed_terms_over_trajectory.update(hits)
                query_exposed_terms_over_trajectory.update(query_hits)
                schema_exposed_terms_over_trajectory.update(schema_hits)
            call_reports.append(
                {
                    "call_index": call_index,
                    "query_exposed_terms": query_hits,
                    "schema_exposed_terms": schema_hits,
                    "exposed_terms": hits,
                    "has_direct_exposure": bool(hits),
                }
            )

        exposed_entity_occurrences += len(exposed_terms_over_trajectory)
        per_case.append(
            {
                "case_id": case_id,
                "db_id": row.get("db_id"),
                "num_gold_sensitive_entities": len(terms),
                "num_cloud_calls": len(calls),
                "num_retries": max(len(calls) - 1, 0),
                "has_any_direct_exposure": bool(exposed_terms_over_trajectory),
                "has_query_direct_exposure": bool(query_exposed_terms_over_trajectory),
                "has_schema_direct_exposure": bool(schema_exposed_terms_over_trajectory),
                "exposed_terms_over_complete_trajectory": sorted(
                    exposed_terms_over_trajectory, key=normalize_surface
                ),
                "calls": call_reports,
            }
        )

    num_cases = len(per_case)
    cases_with_cloud_calls = sum(row["num_cloud_calls"] > 0 for row in per_case)
    cases_with_retries = sum(row["num_retries"] > 0 for row in per_case)
    exposed_cases = sum(row["has_any_direct_exposure"] for row in per_case)
    query_exposed_cases = sum(row["has_query_direct_exposure"] for row in per_case)
    schema_exposed_cases = sum(row["has_schema_direct_exposure"] for row in per_case)
    query_and_schema_exposed_cases = sum(
        row["has_query_direct_exposure"] and row["has_schema_direct_exposure"]
        for row in per_case
    )
    aggregate = {
        "definition": (
            "A case is directly exposed if any gold original sensitive entity "
            "appears by normalized exact-surface match in the query or schema of "
            "any actual cloud SQL-generation call across the complete trajectory."
        ),
        "num_cases": num_cases,
        "cases_with_cloud_calls": cases_with_cloud_calls,
        "cases_without_cloud_calls": num_cases - cases_with_cloud_calls,
        "total_cloud_calls": total_cloud_calls,
        "cases_with_cloud_retries": cases_with_retries,
        "max_cloud_calls_per_case": max((row["num_cloud_calls"] for row in per_case), default=0),
        "cloud_call_count_distribution": {
            str(k): v for k, v in sorted(call_count_distribution.items())
        },
        "cases_with_any_direct_exposure": exposed_cases,
        "cases_with_query_direct_exposure": query_exposed_cases,
        "cases_with_schema_direct_exposure": schema_exposed_cases,
        "cases_with_query_and_schema_direct_exposure": query_and_schema_exposed_cases,
        "question_level_direct_exposure_rate_all_cases": (
            exposed_cases / num_cases if num_cases else 0.0
        ),
        "question_level_direct_exposure_rate_cloud_called_cases": (
            exposed_cases / cases_with_cloud_calls if cases_with_cloud_calls else 0.0
        ),
        "cloud_calls_with_any_direct_exposure": leaky_cloud_calls,
        "cloud_call_direct_exposure_rate": (
            leaky_cloud_calls / total_cloud_calls if total_cloud_calls else 0.0
        ),
        "total_gold_sensitive_entities": total_sensitive_entities,
        "unique_exposed_entity_case_pairs": exposed_entity_occurrences,
        "entity_level_direct_exposure_rate": (
            exposed_entity_occurrences / total_sensitive_entities
            if total_sensitive_entities
            else 0.0
        ),
    }
    return {"aggregate": aggregate, "per_case": per_case}


def markdown_summary(report: Dict[str, Any], source: Path) -> str:
    a = report["aggregate"]
    pct = lambda value: f"{100.0 * value:.2f}%"
    return "\n".join(
        [
            "# Complete outbound trajectory privacy audit",
            "",
            f"Source: `{source}`",
            "",
            "## Rule",
            "",
            a["definition"],
            "Gold labels are used only for retrospective evaluation and are not available to the inference-time gate.",
            "",
            "## Results",
            "",
            "| Item | Result |",
            "|---|---:|",
            f"| Evaluated questions | {a['num_cases']} |",
            f"| Questions with at least one cloud call | {a['cases_with_cloud_calls']} |",
            f"| Total cloud calls | {a['total_cloud_calls']} |",
            f"| Questions with cloud retries | {a['cases_with_cloud_retries']} |",
            f"| Maximum cloud calls for one question | {a['max_cloud_calls_per_case']} |",
            f"| Questions exposing at least one original sensitive entity | {a['cases_with_any_direct_exposure']} |",
            f"| Questions with exposure in the outbound query | {a['cases_with_query_direct_exposure']} |",
            f"| Questions with exposure in the outbound schema | {a['cases_with_schema_direct_exposure']} |",
            f"| Questions with exposure in both | {a['cases_with_query_and_schema_direct_exposure']} |",
            f"| Question-level direct exposure rate | {pct(a['question_level_direct_exposure_rate_all_cases'])} |",
            f"| Cloud calls containing direct exposure | {a['cloud_calls_with_any_direct_exposure']} |",
            f"| Call-level direct exposure rate | {pct(a['cloud_call_direct_exposure_rate'])} |",
            f"| Original sensitive entities | {a['total_gold_sensitive_entities']} |",
            f"| Exposed entity-case pairs | {a['unique_exposed_entity_case_pairs']} |",
            f"| Entity-level direct exposure rate | {pct(a['entity_level_direct_exposure_rate'])} |",
            "",
            "The denominator of the headline question-level rate is all evaluated questions. "
            "A question with no cloud call is counted as not directly exposed.",
            "",
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--all-res", type=Path, required=True)
    parser.add_argument("--case-logs", type=Path, default=None)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    case_logs = args.case_logs or args.all_res.parent / "case_logs"
    report = audit(args.all_res, case_logs)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_md.write_text(markdown_summary(report, args.all_res), encoding="utf-8")
    print(json.dumps(report["aggregate"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
