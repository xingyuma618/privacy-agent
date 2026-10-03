#!/usr/bin/env python3
"""Conservative projection cleanup for MAC-SQL Spider outputs.

Some CRR outputs include helper aggregate columns used only for ranking, e.g.
`SELECT manufacturer, COUNT(*) AS cnt ... ORDER BY cnt`. Spider gold often
expects only `manufacturer`. This script removes such trailing helper columns
only when the natural-language question contains frequency/ranking cues and
keeps ORDER BY valid by replacing aliases with their aggregate expression.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import List


RANKING_CUES = [
    "most common",
    "most frequent",
    "most popular",
    "most uncommon",
    "appeared most frequently",
    "appears most often",
    "most number",
    "least number",
    "fewest",
    "smallest frequency",
    "frequency of occurrence",
    "descending order of count",
    "descending order of their frequency",
    "ordered descending by the average",
    "with most",
    "used the most",
    "has the most",
    "have the most",
    "that has the most",
    "most types",
    "least common",
    "shared by more than",
]


def read_jsonl(path: Path) -> List[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_jsonl(path: Path, rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")


def split_select(expr: str) -> List[str]:
    parts, cur = [], []
    depth = 0
    quote = None
    for ch in expr:
        if quote:
            cur.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in "'\"`":
            quote = ch
            cur.append(ch)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")" and depth > 0:
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur).strip())
    return parts


def alias_expr(part: str) -> tuple[str, str | None]:
    match = re.match(r"(.+?)\s+as\s+[`\"\[]?([A-Za-z_][\w]*)[`\"\]]?\s*$", part, re.I | re.S)
    if match:
        return match.group(1).strip(), match.group(2)
    return part, None


def replace_order_alias(sql: str, alias: str | None, expr: str) -> str:
    if not alias:
        return sql
    return re.sub(
        r"(\border\s+by\s+)([`\"\[]?)" + re.escape(alias) + r"([`\"\]]?)(\b)",
        lambda match: match.group(1) + expr + match.group(4),
        sql,
        flags=re.I,
    )


def rebuild_select(sql: str, keep_parts: List[str]) -> str:
    match = re.search(r"\bselect\b(.*?)\bfrom\b", sql, re.I | re.S)
    if not match:
        return sql
    return sql[: match.start(1)] + " " + ", ".join(keep_parts) + " " + sql[match.end(1) :]


def should_fix(question: str) -> bool:
    q = (question or "").lower()
    if q.startswith("how many"):
        return False
    return any(cue in q for cue in RANKING_CUES)


def fix_sql(sql: str, question: str) -> str:
    if not should_fix(question):
        return sql
    match = re.search(r"\bselect\b(.*?)\bfrom\b", sql, re.I | re.S)
    if not match:
        return sql
    keep = split_select(match.group(1))
    if len(keep) <= 1:
        return sql
    new_sql = sql
    changed = False
    while len(keep) > 1:
        expr, alias = alias_expr(keep[-1])
        if not re.search(r"\b(count|avg|sum|min|max)\s*\(", expr, re.I):
            break
        new_sql = replace_order_alias(new_sql, alias, expr)
        keep.pop()
        changed = True
    return rebuild_select(new_sql, keep) if changed else sql


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-jsonl", type=Path, required=True)
    parser.add_argument("--output-jsonl", type=Path, required=True)
    args = parser.parse_args()

    rows = read_jsonl(args.input_jsonl)
    changed = 0
    out = []
    for row in rows:
        new_row = dict(row)
        original = str(row.get("pred") or row.get("final_sql") or "")
        fixed = fix_sql(original, str(row.get("query") or ""))
        if fixed != original:
            changed += 1
            new_row["pred_original"] = original
            new_row["postprocess_changed"] = True
            new_row["pred"] = fixed
            new_row["final_sql"] = fixed
        else:
            new_row["postprocess_changed"] = False
        out.append(new_row)
    write_jsonl(args.output_jsonl, out)
    print(json.dumps({"rows": len(rows), "changed": changed, "output": str(args.output_jsonl)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
