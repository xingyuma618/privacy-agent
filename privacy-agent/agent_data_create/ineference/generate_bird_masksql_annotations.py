#!/usr/bin/env python3
"""Generate MaskSQL-style privacy annotations for full BIRD-dev.

The released BIRD privacy subset in this workspace contains only 300 examples.
This script prepares the missing full-dev annotations in the same structural
format used by MaskSQL-derived files:

  orig_gold_schema_links: {surface: "TABLE:[t]" or "COLUMN:[t].[c]"}
  orig_gold_value_links: {surface: "[t].[c]"}

It is deliberately resumable because the full 1,534-example BIRD-dev pass
requires remote model calls.
"""

from __future__ import annotations

# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))


import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None

try:
    import httpx
except Exception:  # pragma: no cover
    httpx = None


DEFAULT_INPUT = Path(str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data/bird_dev_sc.jsonl'))
DEFAULT_OUT = Path(
    str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data/bird_dev_full_masksql_generated.jsonl')
)


SYSTEM_PROMPT = """You create MaskSQL-style privacy annotations for Text-to-SQL.

Given a database schema, a natural-language question, optional evidence, and the
gold SQL, identify all sensitive surface phrases in the question/evidence that
refer to database schema items or literal cell values.

Return ONLY a JSON object with exactly two keys:
{
  "orig_gold_schema_links": {
    "<surface phrase>": "TABLE:[<table>]" or "COLUMN:[<table>].[<column>]"
  },
  "orig_gold_value_links": {
    "<surface phrase>": "[<table>].[<column>]"
  }
}

Rules:
- Use only surface phrases that appear verbatim in the question or evidence.
- For schema links, include table mentions and column mentions needed for the SQL.
- For values, the JSON key must be the exact phrase from the question/evidence.
  If the SQL uses a normalized code/value that is implied by a natural-language
  phrase, use the natural-language phrase as the key, not the hidden SQL value.
- Keep surface phrases minimal. Strip comparative or aggregation words unless
  they are part of the actual schema/value mention.
- Use exact table and column names from the schema/SQL.
- Do not include SQL operators, aggregation words, or generic logic words as
  sensitive terms.
- If no links exist for a category, output an empty object for that category.
"""


USER_TEMPLATE = """Database Schema:
{schema}

Question:
{question}

Evidence:
{evidence}

Gold SQL:
{sql}
"""


def read_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return json.loads(path.read_text(encoding="utf-8"))


def load_completed(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        rows[str(row["idx"])] = row
    return rows


def append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise
        parsed = json.loads(match.group(0))
    if not isinstance(parsed, dict):
        raise ValueError("annotation response is not a JSON object")
    return parsed


def surface_allowed(surface: str, source_text: str) -> bool:
    return surface.lower() in source_text.lower()


def normalize_schema_links(raw: Any, source_text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    for surface, target in raw.items():
        surface = str(surface).strip()
        target = str(target).strip()
        if not surface or not target or not surface_allowed(surface, source_text):
            continue
        if re.fullmatch(r"TABLE:\[[^\]]+\]", target, flags=re.IGNORECASE) or re.fullmatch(
            r"COLUMN:\[[^\]]+\]\.\[[^\]]+\]", target, flags=re.IGNORECASE
        ):
            out[surface] = target
    return out


def normalize_value_links(raw: Any, source_text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    for surface, target in raw.items():
        surface = str(surface).strip()
        target = str(target).strip()
        if surface and surface_allowed(surface, source_text) and re.fullmatch(r"\[[^\]]+\]\.\[[^\]]+\]", target):
            out[surface] = target
    return out


def make_output_row(idx: int, source: dict[str, Any], annotation: dict[str, Any], raw: str) -> dict[str, Any]:
    source_text = f"{source.get('question', '')}\n{source.get('evidence', '')}"
    schema_links = normalize_schema_links(annotation.get("orig_gold_schema_links"), source_text)
    value_links = normalize_value_links(annotation.get("orig_gold_value_links"), source_text)
    return {
        "idx": f"bird_{idx}",
        "db_id": "<DB_ID>",
        "orig_db_id": source.get("db_id", ""),
        "question": "<QUESTION>",
        "orig_question": source.get("question", ""),
        "orig_masked": "",
        "evidence": "<EVIDENCE>",
        "orig_evidence": source.get("evidence", ""),
        "gold_schema_links": "<GOLD_SCHEMA_LINKS>",
        "orig_gold_schema_links": schema_links,
        "gold_value_links": "<GOLD_VALUE_LINKS>",
        "orig_gold_value_links": value_links,
        "gold_sql": "<GOLD_SQL>",
        "orig_gold_sql": source.get("gold_sql") or source.get("SQL") or "",
        "evidence_added": bool(source.get("evidence")),
        "_annotation_model_raw": raw,
    }


def annotate_one(client: Any, model: str, idx: int, row: dict[str, Any], retries: int) -> dict[str, Any]:
    prompt = USER_TEMPLATE.format(
        schema=row.get("db_schema", ""),
        question=row.get("question", ""),
        evidence=row.get("evidence", ""),
        sql=row.get("gold_sql") or row.get("SQL") or "",
    )
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            text = response.choices[0].message.content or "{}"
            annotation = extract_json_object(text)
            return make_output_row(idx, row, annotation, text)
        except Exception as exc:  # pragma: no cover - network/model dependent
            last_error = exc
            time.sleep(min(2**attempt, 16))
    raise RuntimeError(f"failed to annotate bird_{idx}: {last_error}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--base-url", default=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"))
    parser.add_argument("--model", default=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"))
    parser.add_argument("--api-key", default=os.environ.get("DEEPSEEK_API_KEY"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = read_rows(args.input)
    selected = list(enumerate(rows))[args.start :]
    if args.limit is not None:
        selected = selected[: args.limit]

    if args.dry_run:
        print(
            json.dumps(
                {
                    "input": str(args.input),
                    "output": str(args.output),
                    "total_input_rows": len(rows),
                    "selected_rows": len(selected),
                    "first_selected_idx": selected[0][0] if selected else None,
                    "first_question": selected[0][1].get("question") if selected else None,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if OpenAI is None:
        raise RuntimeError("openai package is required")
    if not args.api_key:
        raise RuntimeError("DEEPSEEK_API_KEY or --api-key is required")

    completed = {} if args.force else load_completed(args.output)
    pending = [(idx, row) for idx, row in selected if f"bird_{idx}" not in completed]
    print(f"selected={len(selected)} completed={len(completed)} pending={len(pending)}", flush=True)

    http_client = httpx.Client(trust_env=False) if httpx is not None else None
    client = OpenAI(api_key=args.api_key, base_url=args.base_url, http_client=http_client)
    buffer: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(annotate_one, client, args.model, idx, row, args.retries): idx
            for idx, row in pending
        }
        for done_count, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            buffer.append(result)
            if len(buffer) >= 20 or done_count == len(futures):
                buffer.sort(key=lambda item: int(str(item["idx"]).split("_")[-1]))
                append_jsonl(args.output, buffer)
                buffer.clear()
                print(f"written={done_count}/{len(futures)}", flush=True)


if __name__ == "__main__":
    main()
