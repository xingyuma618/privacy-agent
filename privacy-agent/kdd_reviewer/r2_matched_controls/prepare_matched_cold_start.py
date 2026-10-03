#!/usr/bin/env python3
"""Create equal-size Prompt-Synthesis and MCTS cold-start datasets.

The script keeps every MCTS trajectory (3,101 in the paper run) and draws the
same number of Prompt-Synthesis trajectories with a fixed seed.  It never
changes record contents.  A manifest records input hashes and selected row
indices so the comparison can be reproduced exactly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from pathlib import Path
from typing import Any


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(f"Invalid JSON at {path}:{line_number}: {error}") from error
    return rows


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def trajectory_stats(rows: list[dict[str, Any]]) -> dict[str, float | int]:
    message_counts = [len(row.get("messages", [])) for row in rows]
    assistant_chars = []
    tool_markers = []
    for row in rows:
        messages = row.get("messages", [])
        assistant_text = "\n".join(
            str(message.get("content") or "")
            for message in messages
            if message.get("role") == "assistant"
        )
        assistant_chars.append(len(assistant_text))
        tool_markers.append(assistant_text.count("<tool_call>"))
    n = len(rows)
    return {
        "rows": n,
        "mean_messages": sum(message_counts) / n if n else 0.0,
        "mean_assistant_chars": sum(assistant_chars) / n if n else 0.0,
        "mean_tool_calls": sum(tool_markers) / n if n else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prompt", type=Path, required=True)
    parser.add_argument("--mcts", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()

    prompt_rows = read_jsonl(args.prompt)
    mcts_rows = read_jsonl(args.mcts)
    if len(prompt_rows) < len(mcts_rows):
        raise ValueError(
            f"Prompt set has {len(prompt_rows)} rows, fewer than MCTS ({len(mcts_rows)})."
        )

    rng = random.Random(args.seed)
    selected_indices = sorted(rng.sample(range(len(prompt_rows)), len(mcts_rows)))
    matched_prompt_rows = [prompt_rows[index] for index in selected_indices]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    prompt_output = args.output_dir / "prompt_synthesis_n3101.jsonl"
    mcts_output = args.output_dir / "mcts_synthesis_n3101.jsonl"
    write_jsonl(prompt_output, matched_prompt_rows)
    write_jsonl(mcts_output, mcts_rows)

    manifest = {
        "seed": args.seed,
        "selection_rule": "Keep all MCTS rows; sample Prompt-Synthesis rows without replacement.",
        "prompt_input": str(args.prompt.resolve()),
        "prompt_input_rows": len(prompt_rows),
        "prompt_input_sha256": sha256(args.prompt),
        "mcts_input": str(args.mcts.resolve()),
        "mcts_input_rows": len(mcts_rows),
        "mcts_input_sha256": sha256(args.mcts),
        "matched_rows_per_method": len(mcts_rows),
        "selected_prompt_indices": selected_indices,
        "prompt_output_sha256": sha256(prompt_output),
        "mcts_output_sha256": sha256(mcts_output),
        "prompt_stats": trajectory_stats(matched_prompt_rows),
        "mcts_stats": trajectory_stats(mcts_rows),
    }
    with (args.output_dir / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)

    print(json.dumps({key: value for key, value in manifest.items() if key != "selected_prompt_indices"}, indent=2))


if __name__ == "__main__":
    main()
