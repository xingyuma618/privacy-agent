#!/usr/bin/env python3
"""Regenerate SQL from already-masked Spider inputs.

The script preserves the privacy surface of the agent run: only masked questions
and masked schemas are sent to the SQL model. It then unpacks the generated
masked SQL locally with the per-example unmask dictionary.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List

import httpx
from openai import OpenAI


SYSTEM_PROMPT = """You are a careful Text-to-SQL assistant for SQLite.
The database schema and question are privacy-masked: table_*, col_*, and val_* are real schema/value symbols.
Return only one executable SQLite SQL query, with no markdown and no explanation.

Rules:
- Use only identifiers that appear in the masked schema.
- Preserve exactly the answer columns requested by the question, in the same order.
- Do not output helper columns used only for GROUP BY, ORDER BY, filtering, or counting unless the question asks for them.
- For "most/common/highest/lowest/youngest/oldest", use ORDER BY with LIMIT 1 unless the question asks for all ties.
- For "for each", GROUP BY the entity and output the requested entity plus requested aggregate.
- For "distinct/different", use DISTINCT.
- Prefer INTERSECT/EXCEPT when the question asks for entities satisfying two separate set conditions.
- Quote identifiers with backticks.
"""


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def extract_sql(text: str) -> str:
    text = (text or "").strip()
    match = re.search(r"```(?:sql)?\s*(.*?)```", text, re.I | re.S)
    if match:
        text = match.group(1).strip()
    text = re.sub(r"^\s*SQL\s*:\s*", "", text, flags=re.I)
    return text.strip().rstrip(";") + ";"


def quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def repair_qualified_identifier_quotes(sql: str, unmask_dict: Dict[str, Any] | None = None) -> str:
    if not sql:
        return sql
    columns = set()
    for original in ((unmask_dict or {}).get("columns") or {}).values():
        original_text = str(original)
        if "." in original_text:
            columns.add(original_text)

    def repl(match: re.Match[str]) -> str:
        table_col = match.group(1)
        if table_col not in columns:
            return match.group(0)
        table, col = table_col.split(".", 1)
        return f"{quote_identifier(table)}.{quote_identifier(col)}"

    sql = re.sub(r'"([A-Za-z_][\w]*\.[A-Za-z_][\w]*)"', repl, sql)
    sql = re.sub(r'`([A-Za-z_][\w]*\.[A-Za-z_][\w]*)`', repl, sql)
    sql = re.sub(r'\[([A-Za-z_][\w]*\.[A-Za-z_][\w]*)\]', repl, sql)
    return sql


def unmask_sql(masked_sql: str, unmask_dict: Dict[str, Any]) -> str:
    mask_info: List[Dict[str, str]] = []
    for category in ["tables", "columns", "values"]:
        for mask, original in (unmask_dict.get(category) or {}).items():
            mask_info.append({"mask": str(mask), "original": str(original), "category": category})
    mask_info.sort(key=lambda item: len(item["mask"]), reverse=True)

    sql = masked_sql
    for item in mask_info:
        mask = item["mask"]
        original = item["original"]
        category = item["category"]
        pattern = re.compile(r"\b" + re.escape(mask) + r"\b", re.I)

        def replace(match: re.Match[str]) -> str:
            left = match.string[match.start() - 1] if match.start() > 0 else ""
            right = match.string[match.end()] if match.end() < len(match.string) else ""
            before = match.string[: match.start()]
            value = original
            if category == "columns" and "." in value:
                table, col = value.split(".", 1)
                # In masked SQL the model often emits aliases such as
                # `t1`.`col_3`.  In that context the mask must become just the
                # real column name; otherwise unmasking creates invalid
                # three-part identifiers like `t1`.`table`.`column`.
                qualified_by_alias = left == "." or re.search(r"(?:\b\w+|`[^`]+`|\"[^\"]+\")\s*\.\s*[`\"]?$", before)
                alias_target = re.search(r"\bAS\s+[`\"]?$", before, flags=re.IGNORECASE)
                if qualified_by_alias or alias_target:
                    value = col
                elif left not in ("`", "'", '"') and right not in ("`", "'", '"'):
                    return f"{quote_identifier(table)}.{quote_identifier(col)}"
            if category in {"tables", "columns"} and left not in ("`", "'", '"') and right not in ("`", "'", '"'):
                return quote_identifier(value)
            return value

        sql = pattern.sub(replace, sql)
    return repair_qualified_identifier_quotes(sql, unmask_dict)


def build_messages(row: Dict[str, Any]) -> List[Dict[str, str]]:
    user = (
        "Masked database schema:\n"
        f"{row.get('masked_schema', '')}\n\n"
        "Masked question:\n"
        f"{row.get('masked_question', '')}\n"
    )
    return [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]


def regenerate_one(client: OpenAI, model: str, row: Dict[str, Any]) -> Dict[str, Any]:
    response = client.chat.completions.create(
        model=model,
        messages=build_messages(row),
        temperature=0,
    )
    masked_sql = extract_sql(response.choices[0].message.content or "")
    pred_sql = unmask_sql(masked_sql, row.get("unmask_dict") or {})
    out = dict(row)
    out["masked_sql_regen"] = masked_sql
    out["pred_sql_before_regen"] = row.get("pred_sql", "")
    out["pred_sql"] = pred_sql
    return out


def remask_saved_sql(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for row in rows:
        new_row = dict(row)
        masked_sql = row.get("masked_sql_regen") or row.get("masked_sql") or ""
        if masked_sql:
            new_row["pred_sql_before_reunmask"] = row.get("pred_sql", "")
            new_row["pred_sql"] = unmask_sql(masked_sql, row.get("unmask_dict") or {})
        results.append(new_row)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--base-url", default=os.environ.get("SQL_GENERATE_BASE_URL", "https://api.deepseek.com/v1"))
    parser.add_argument("--model", default=os.environ.get("SQL_GENERATE_MODEL_NAME", "deepseek-chat"))
    parser.add_argument("--api-key", default=os.environ.get("SQL_GENERATE_API_KEY", "EMPTY"))
    parser.add_argument("--max-workers", type=int, default=int(os.environ.get("MAX_WORKERS", "16")))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--only-indices", type=Path, default=None)
    parser.add_argument("--reunmask-only", action="store_true", help="Re-unmask saved masked_sql_regen without API calls.")
    args = parser.parse_args()

    rows = read_json(args.input)
    if args.reunmask_only:
        write_json(args.output, remask_saved_sql(rows))
        print(json.dumps({"rows": len(rows), "reunmasked": len(rows), "output": str(args.output)}, ensure_ascii=False, indent=2))
        return

    if args.only_indices:
        selected = {int(x) for x in read_json(args.only_indices)}
    else:
        selected = set(range(len(rows[: args.limit] if args.limit else rows)))
    if args.limit:
        selected = {idx for idx in selected if idx < args.limit}

    client = OpenAI(api_key=args.api_key, base_url=args.base_url, http_client=httpx.Client(trust_env=False))
    results = list(rows)
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(regenerate_one, client, args.model, rows[idx]): idx
            for idx in sorted(selected)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as exc:
                failed = dict(rows[idx])
                failed["regen_error"] = repr(exc)
                results[idx] = failed
    write_json(args.output, results)
    print(json.dumps({"rows": len(rows), "regenerated": len(selected), "output": str(args.output)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
