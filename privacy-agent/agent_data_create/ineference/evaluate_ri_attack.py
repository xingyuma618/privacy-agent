#!/usr/bin/env python3
"""Run MaskSQL-style re-identification attacks on saved masked prompts."""

import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
import yaml
from openai import OpenAI

from agent_inference_with_privacy_eval_v4 import (
    _run_reidentification_attack,
    compute_privacy_metrics_for_result,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--config", help="MaskSQL YAML containing attack model credentials")
    parser.add_argument("--base-url")
    parser.add_argument("--model")
    parser.add_argument("--api-key", default=os.environ.get("RI_ATTACK_API_KEY"))
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--limit", type=int, help="Evaluate only the first N cases (smoke test)")
    parser.add_argument("--attack-field", help="Score saved attacker text instead of calling an API")
    parser.add_argument("--force", action="store_true", help="Ignore an existing output report")
    return parser.parse_args()


def load_credentials(args):
    config = {}
    model = args.model
    if args.config:
        with open(args.config, encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
        config = raw.get("models", {}).get("attack", raw.get("attack", {}))
        if not config:
            config = raw.get("llm_openai", {})
            model = model or raw.get("llm")
    base_url = args.base_url or config.get("base_url")
    model = model or config.get("model") or config.get("model_name")
    api_key = args.api_key or config.get("api_key") or os.environ.get(config.get("api_key_env", ""))
    if not all((base_url, model, api_key)):
        raise ValueError("Attack base URL, model, and API key are required")
    return base_url, model, api_key


def load_completed(path):
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        report = json.load(handle)
    return {str(row["case_id"]): row for row in report.get("per_case", []) if row.get("attack_text")}


def write_report(path, rows, total_cases):
    ordered = sorted(rows.values(), key=lambda row: int(row.get("eval_order", 0)))
    ri_values = [row["RI"] for row in ordered if row.get("RI") is not None]
    report = {
        "aggregate": {
            "num_cases": total_cases,
            "num_attacked": len(ri_values),
            "RI": sum(ri_values) / len(ri_values) if ri_values else None,
        },
        "per_case": ordered,
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    os.replace(temporary, path)


def main():
    args = parse_args()
    base_url = model = api_key = None
    if not args.attack_field:
        base_url, model, api_key = load_credentials(args)
    with open(args.input, encoding="utf-8") as handle:
        cases = json.load(handle)
    if args.limit is not None:
        cases = cases[: args.limit]
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    completed = {} if args.force else load_completed(output)
    lock = threading.Lock()
    local = threading.local()

    def attack(index_case):
        index, case = index_case
        case_id = str(case.get("case_id", index))
        if case_id in completed:
            return case_id, completed[case_id]
        if args.attack_field:
            text = str(case.get(args.attack_field, ""))
            metrics = compute_privacy_metrics_for_result(case, attack_text=text)
            metrics.update({"case_id": case_id, "eval_order": index, "attack_text": text})
            return case_id, metrics
        if not case.get("masked_terms"):
            metrics = compute_privacy_metrics_for_result(case, attack_text="")
            metrics.update({"case_id": case_id, "eval_order": index, "RI": 1.0, "attack_text": "[NO_MASKED_TERMS]"})
            return case_id, metrics
        if not hasattr(local, "client"):
            local.client = OpenAI(
                api_key=api_key,
                base_url=base_url,
                timeout=args.timeout,
                max_retries=0,
                http_client=httpx.Client(trust_env=False),
            )
        error = None
        for attempt in range(args.retries):
            try:
                text = _run_reidentification_attack(
                    local.client, model, case.get("masked_question", ""), case.get("masked_schema", "")
                )
                metrics = compute_privacy_metrics_for_result(case, attack_text=text)
                metrics.update({"case_id": case_id, "eval_order": index, "attack_text": text})
                return case_id, metrics
            except Exception as exc:
                error = exc
                time.sleep(min(2 ** attempt, 16))
        raise RuntimeError(f"case {case_id} failed after {args.retries} attempts: {error}")

    pending = [(index, case) for index, case in enumerate(cases) if str(case.get("case_id", index)) not in completed]
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(attack, item): item[0] for item in pending}
        for done_count, future in enumerate(as_completed(futures), start=1):
            case_id, row = future.result()
            with lock:
                completed[case_id] = row
                if done_count % 25 == 0 or done_count == len(pending):
                    write_report(output, completed, len(cases))
                    print(f"completed {len(completed)}/{len(cases)}", flush=True)
    write_report(output, completed, len(cases))
    print(json.dumps(json.load(output.open(encoding="utf-8"))["aggregate"], indent=2))


if __name__ == "__main__":
    main()
