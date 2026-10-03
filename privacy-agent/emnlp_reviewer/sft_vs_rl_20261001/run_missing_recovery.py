#!/usr/bin/env python3
"""Assemble SFT repetition 1 from 299 persisted rows plus one real missing inference."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
import time
from concurrent.futures import Future
from pathlib import Path

import runner


SOURCE = (runner.HERE / "results_local_qwen_partial_archive" / "sft-run-1-interrupted-299-20261001").resolve()
FORMAL_OUTPUT = (runner.HERE / "results_local_qwen" / "sft" / "run-1").resolve()
MISSING_IDS = ["bird_122"]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("model", choices=("sft",))
    parser.add_argument("run", type=int, choices=(1,))
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--execute-reviewed", action="store_true")
    args = parser.parse_args()
    if args.workers != 8 or args.limit != 0 or not args.execute_reviewed:
        raise SystemExit("missing-only recovery is restricted to formal SFT run 1 with workers=8 and limit=0")
    if Path(args.output).resolve() != FORMAL_OUTPUT:
        raise SystemExit(f"output must be the formal run-1 directory: {FORMAL_OUTPUT}")

    manifest_path = runner.HERE / "manifest.json"
    source_cases_path = SOURCE / "cases.jsonl"
    source_metadata_path = SOURCE / "metadata.json"
    source_metadata = json.loads(source_metadata_path.read_text())
    source_rows = [json.loads(line) for line in source_cases_path.read_text().splitlines()]
    if source_metadata.get("model") != "sft" or source_metadata.get("run") != 1:
        raise RuntimeError("source is not SFT repetition 1")
    if source_metadata.get("completed_cases") != 299 or len(source_rows) != 299:
        raise RuntimeError("source must contain exactly 299 persisted cases")
    if source_metadata.get("workers") != 8 or source_metadata.get("limit") != 0:
        raise RuntimeError("source execution settings differ from formal repetition 1")
    if source_metadata.get("manifest_sha256") != sha256(manifest_path):
        raise RuntimeError("source manifest hash differs from the frozen manifest")
    if source_metadata.get("runner_sha256") != sha256(runner.HERE / "runner.py"):
        raise RuntimeError("source runner hash differs from the frozen runner")
    if any(row.get("task_status") != "valid" for row in source_rows):
        raise RuntimeError("all retained source rows must be valid")

    by_id = {str(row["question_id"]): row for row in source_rows}
    if len(by_id) != 299:
        raise RuntimeError("source question IDs are not unique")
    canonical_ids = [str(value) for value in source_metadata["question_ids"]]
    missing = [question_id for question_id in canonical_ids if question_id not in by_id]
    if missing != MISSING_IDS or set(by_id) != set(canonical_ids) - set(MISSING_IDS):
        raise RuntimeError(f"source missing-ID set differs from the approved recovery: {missing}")

    recovery_started_unix = time.time()
    driver_sha256 = sha256(Path(__file__))
    provenance = {
        "source_directory": str(SOURCE),
        "source_cases_sha256": sha256(source_cases_path),
        "source_metadata_sha256": sha256(source_metadata_path),
        "retained_question_ids": [question_id for question_id in canonical_ids if question_id in by_id],
        "missing_question_ids": MISSING_IDS,
        "driver_sha256": driver_sha256,
        "source_model": "sft",
        "source_run": 1,
    }

    saved_executor = runner.concurrent.futures.ThreadPoolExecutor
    saved_write_json = runner.write_json

    class SegmentedExecutor:
        def __init__(self, max_workers=None, *executor_args, **executor_kwargs):
            if max_workers != 8:
                raise RuntimeError(f"unexpected executor worker count: {max_workers}")
            self.real = saved_executor(max_workers=max_workers, *executor_args, **executor_kwargs)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return self.real.__exit__(exc_type, exc, traceback)

        def submit(self, function, pair):
            _, item = pair
            question_id = str(item["masksql_idx"])
            if question_id in by_id:
                future = Future()
                future.set_result(copy.deepcopy(by_id[question_id]))
                return future
            if question_id != MISSING_IDS[0]:
                raise RuntimeError(f"unapproved missing inference: {question_id}")
            return self.real.submit(function, pair)

    def recovery_write_json(path, value):
        if path.name == "metadata.json" and isinstance(value, dict):
            value = dict(value)
            value["started_unix"] = source_metadata["started_unix"]
            value["recovery_started_unix"] = recovery_started_unix
            value["inference_origin"] = "segmented_recovery"
            value["recovery_provenance"] = provenance
        return saved_write_json(path, value)

    runner.concurrent.futures.ThreadPoolExecutor = SegmentedExecutor
    runner.write_json = recovery_write_json
    runner.main()

    assembled_rows = [json.loads(line) for line in (FORMAL_OUTPUT / "cases.jsonl").read_text().splitlines()]
    assembled_by_id = {str(row["question_id"]): row for row in assembled_rows}
    if len(assembled_rows) != 300 or len(assembled_by_id) != 300:
        raise RuntimeError("assembled formal result is not 300 unique rows")
    for question_id, source_row in by_id.items():
        if assembled_by_id[question_id] != source_row:
            raise RuntimeError(f"retained row changed during assembly: {question_id}")
    if assembled_by_id[MISSING_IDS[0]].get("task_status") != "valid":
        raise RuntimeError("new missing inference is not valid")


if __name__ == "__main__":
    main()
