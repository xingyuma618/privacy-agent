#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Experiment runner for PrivAgent-R1 ablations and extended evaluation.

Place this file in the same directory as agent_inference_with_privacy_eval_v4.py.
It reuses your current inference/evaluation code and adds:
1. Module ablations.
2. Fixed-pipeline baseline for w/o agentic refinement.
3. Privacy-utility variants.
4. Extra metrics: ExecAcc, ValidSQL, Privacy-P/R/F1, LeakRate, RI/ASR,
   SemSim proxy, AvgToolCalls, AvgRounds, AvgLatencySec.
5. Uniform checkpoint evaluation for MCTS/reward ablations.
6. Optional MaskSQL-style re-identification attack aggregation into summary files.
"""

import argparse
import copy
import csv
import json
import logging
import os
import re
import sqlite3
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import json_repair
from openai import OpenAI

# Put this runner next to your original file.
import agent_inference_with_privacy_eval_v4 as base


@dataclass
class RuntimePaths:
    data_path: str
    db_root_path: str
    output_dir: str
    inference_data_name: str
    diff_json_path: str
    data_mode: str
    ground_truth_path: str
    masksql_test_path: Optional[str] = None
    masksql_match_mode: str = "auto"


@dataclass
class VariantConfig:
    name: str
    base_url: str
    model_name: str
    api_key: str = "EMPTY"
    # Non-zero temperature is useful when running repeated experiments.
    # Keep 0.0 for deterministic evaluation, or set e.g. 0.2 in the config.
    temperature: float = 0.0

    # Inference behavior.
    fixed_pipeline: bool = False                 # w/o agentic refinement.
    disable_query_rewrite: bool = False
    disable_execution_check: bool = False
    untyped_masking: bool = False
    privacy_level: str = "full"                  # low | medium | full.

    # Hide disabled tools from the OpenAI tool list.
    hide_disabled_tools: bool = True

    # Optional re-identification attack.
    run_ri_attack: bool = False
    ri_attack_base_url: Optional[str] = None
    ri_attack_model_name: Optional[str] = None
    ri_attack_api_key: str = "EMPTY"


def safe_mean(vals: List[Any]) -> Optional[float]:
    nums = [v for v in vals if isinstance(v, (int, float)) and v is not None]
    return float(mean(nums)) if nums else None


def expand_env_vars(obj: Any) -> Any:
    """Recursively expand ${VAR} and $VAR in config strings."""
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    if isinstance(obj, list):
        return [expand_env_vars(x) for x in obj]
    if isinstance(obj, dict):
        return {k: expand_env_vars(v) for k, v in obj.items()}
    return obj


def normalize_exec_result(result: Any) -> Any:
    if result is None:
        return None
    if isinstance(result, list):
        try:
            return sorted([tuple(str(x) for x in row) for row in result])
        except Exception:
            return result
    return result


def execute_sql_result(sql: str, db_path: str, timeout_sec: float = 30.0) -> Tuple[bool, Any, str]:
    if not sql or not str(sql).strip():
        return False, None, "empty sql"
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        result = base.safe_execute_sql(conn, sql.strip().rstrip(";"), timeout_sec=timeout_sec)
        return True, normalize_exec_result(result), ""
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}"
    finally:
        if conn is not None:
            conn.close()


def compute_exec_metrics_for_case(res: Dict[str, Any], db_root_path: str) -> Dict[str, Any]:
    db_id = res.get("db_id", "")
    db_path = os.path.join(db_root_path, db_id, f"{db_id}.sqlite")
    pred_sql = res.get("pred_sql", "")
    gold_sql = res.get("gold_sql", "")

    pred_valid, pred_result, pred_err = execute_sql_result(pred_sql, db_path)
    gold_valid, gold_result, gold_err = execute_sql_result(gold_sql, db_path)
    exec_correct = bool(pred_valid and gold_valid and pred_result == gold_result)
    return {
        "valid_sql": 1.0 if pred_valid else 0.0,
        "exec_correct": 1.0 if exec_correct else 0.0,
        "pred_exec_error": pred_err,
        "gold_exec_error": gold_err,
    }


def token_set(text: str) -> set:
    return set(re.findall(r"[A-Za-z0-9_]+", str(text).lower()))


def lexical_semantic_similarity(src: str, tgt: str) -> Optional[float]:
    """Lightweight SemSim proxy. For final paper, you may replace this by BERTScore/SBERT."""
    a, b = token_set(src), token_set(tgt)
    if not a and not b:
        return None
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def add_privacy_extra_metrics(metric: Dict[str, Any]) -> Dict[str, Any]:
    """Add LeakRate and gold-link-based Privacy-P/R/F1 on top of MaskSQL-style metrics."""
    gt = metric.get("gt_sensitive_count", 0) or 0
    covered = metric.get("masked_cover_count", 0) or 0
    predicted = metric.get("masked_terms_count", 0) or 0
    leaks = metric.get("prompt_direct_leak_count", 0) or 0

    p = covered / predicted if predicted else (1.0 if gt == 0 else 0.0)
    r = covered / gt if gt else 1.0
    f1 = (2 * p * r / (p + r)) if (p + r) > 0 else 0.0
    leak_rate = leaks / gt if gt else 0.0

    metric["Privacy-P"] = p
    metric["Privacy-R"] = r
    metric["Privacy-F1"] = f1
    metric["LeakRate"] = leak_rate
    return metric


def parse_trace_metrics_from_log(log_file: str) -> Dict[str, Any]:
    if not os.path.exists(log_file):
        return {
            "tool_calls": 0,
            "rounds": 0,
            "query_rewrite_calls": 0,
            "entity_extract_calls": 0,
            "masking_calls": 0,
            "sql_generate_calls": 0,
            "sql_unmask_calls": 0,
            "execute_check_calls": 0,
            "unknown_tool_calls": 0,
        }
    text = Path(log_file).read_text(encoding="utf-8", errors="ignore")
    tool_names = re.findall(r"\[Action - Tool Call\]:\s*([A-Za-z0-9_]+)", text)
    known = {"query_rewrite", "entity_extract", "apply_typed_masking", "sql_generate", "sql_unmask", "execute_sql_check"}
    return {
        "tool_calls": len(tool_names),
        "rounds": len(re.findall(r"\[Round\s+\d+\]", text)),
        "query_rewrite_calls": tool_names.count("query_rewrite"),
        "entity_extract_calls": tool_names.count("entity_extract"),
        "masking_calls": tool_names.count("apply_typed_masking"),
        "sql_generate_calls": tool_names.count("sql_generate"),
        "sql_unmask_calls": tool_names.count("sql_unmask"),
        "execute_check_calls": tool_names.count("execute_sql_check"),
        "unknown_tool_calls": len([t for t in tool_names if t not in known]),
    }


def filter_entities_by_privacy_level(entities: List[Dict[str, Any]], level: str) -> List[Dict[str, Any]]:
    if not isinstance(entities, list):
        return []
    level = (level or "full").lower()
    if level in {"full", "high"}:
        return entities
    if level == "medium":
        keep_types = {"cell_value", "column", "table"}
        return [e for e in entities if str(e.get("entity_type", "")).lower() in keep_types]
    if level == "low":
        return [e for e in entities if str(e.get("entity_type", "")).lower() == "cell_value"]
    return entities


def replace_mask_tokens(text: str, token_map: Dict[str, str]) -> str:
    if not isinstance(text, str):
        return text
    out = text
    for old, new in sorted(token_map.items(), key=lambda x: len(x[0]), reverse=True):
        out = re.sub(r"\b" + re.escape(old) + r"\b", new, out)
    return out


def convert_to_untyped_masking(mask_res: Dict[str, Any]) -> Dict[str, Any]:
    """Convert table_*/col_*/val_* placeholders into generic mask_* placeholders."""
    if not isinstance(mask_res, dict):
        return mask_res
    old_masks = []
    unmask_dict = copy.deepcopy(mask_res.get("unmask_dict", {}))
    for category in ["tables", "columns", "values"]:
        for m in (unmask_dict.get(category) or {}).keys():
            old_masks.append(m)
    old_masks = sorted(set(old_masks), key=lambda x: (len(x), x), reverse=True)
    token_map = {old: f"mask_{i + 1}" for i, old in enumerate(old_masks)}

    new_res = copy.deepcopy(mask_res)
    new_res["masked_query"] = replace_mask_tokens(new_res.get("masked_query", ""), token_map)
    new_res["masked_schema_str"] = replace_mask_tokens(new_res.get("masked_schema_str", ""), token_map)

    new_unmask = {"tables": {}, "columns": {}, "values": {}}
    for category in ["tables", "columns", "values"]:
        for old_mask, orig in (unmask_dict.get(category) or {}).items():
            new_unmask[category][token_map.get(old_mask, old_mask)] = orig
    new_res["unmask_dict"] = new_unmask

    new_mapping = {}
    for orig, old_mask in (new_res.get("mask_mapping") or {}).items():
        new_mapping[orig] = token_map.get(old_mask, old_mask)
    new_res["mask_mapping"] = new_mapping
    return new_res


class AblationPrivacyProtectionAgent(base.PrivacyProtectionAgent):
    """Agent variant controlled by ablation flags."""

    def __init__(self, variant: VariantConfig):
        super().__init__(base_url=variant.base_url, model_name=variant.model_name, api_key=variant.api_key)
        self.temperature = float(getattr(variant, "temperature", 0.0) or 0.0)
        self.variant = variant
        self._apply_variant_to_prompt_and_tools()

    def _apply_variant_to_prompt_and_tools(self):
        v = self.variant
        if v.hide_disabled_tools:
            disabled = set()
            if v.disable_query_rewrite:
                disabled.add("query_rewrite")
            if v.disable_execution_check:
                disabled.add("execute_sql_check")
            if disabled:
                self.tools = [t for t in self.tools if t.get("function", {}).get("name") not in disabled]

        extra_rules = []
        if v.disable_query_rewrite:
            extra_rules.append("Ablation setting: do NOT call query_rewrite. Use the original question directly.")
        if v.disable_execution_check:
            extra_rules.append("Ablation setting: do NOT call execute_sql_check. After SQL unmasking, directly output the final SQL.")
        if v.untyped_masking:
            extra_rules.append("Ablation setting: use generic untyped masks instead of type-aware placeholders.")
        if v.privacy_level != "full":
            extra_rules.append(f"Ablation setting: privacy strength is {v.privacy_level}.")
        if extra_rules:
            self.system_prompt += "\n\n[Ablation Rules]\n" + "\n".join(f"- {r}" for r in extra_rules) + "\n"

    def _tool_query_rewrite(self, db_schema: str, query_to_rewrite: str, rewrite_instruction: str = "", logger=None) -> str:
        if self.variant.disable_query_rewrite:
            return query_to_rewrite
        return super()._tool_query_rewrite(db_schema, query_to_rewrite, rewrite_instruction, logger=logger)

    def _tool_execute_sql_check(self, unmasked_sql: str, db_path: str) -> str:
        if self.variant.disable_execution_check:
            return "Skipped: execute_sql_check disabled by ablation."
        return super()._tool_execute_sql_check(unmasked_sql, db_path)

    def _tool_apply_typed_masking(self, query, entities, structured_db_schema):
        entities = filter_entities_by_privacy_level(entities, self.variant.privacy_level)
        res = super()._tool_apply_typed_masking(query, entities, structured_db_schema)
        if self.variant.untyped_masking:
            res = convert_to_untyped_masking(res)
        return res


class FixedPipelineAgent(AblationPrivacyProtectionAgent):
    """One-pass deterministic pipeline: approximates w/o agentic refinement."""

    def process(self, item: Dict[str, Any], case_id: str = "default") -> Dict[str, Any]:
        os.makedirs(base.log_dir, exist_ok=True)
        log_file_path = os.path.join(base.log_dir, f"case_{case_id}.txt")
        Path(log_file_path).write_text("", encoding="utf-8")

        def log_to_file(text: str):
            with open(log_file_path, "a", encoding="utf-8") as f:
                f.write(text + "\n")

        original_question = item["question"]
        evidence = item.get("evidence", "")
        question_with_evidence = f"{original_question} {evidence}".strip()
        db_schema = item.get("db_schema", "")
        structured_db_schema = item.get("structured_db_schema", {})
        db_path = os.path.join(base.db_base_dir, item["db_id"], f"{item['db_id']}.sqlite")

        final_question = question_with_evidence
        masked_question = ""
        masked_schema = ""
        masked_sql = ""
        unmask_dict = {}
        masked_terms = []
        mask_mapping = {}
        start_time = time.time()

        try:
            log_to_file("[FixedPipeline] Start")
            if not self.variant.disable_query_rewrite:
                log_to_file("[Action - Tool Call]: query_rewrite")
                final_question = self._tool_query_rewrite(db_schema, question_with_evidence, logger=log_to_file)
                log_to_file(f"[Observation - Result]: {final_question}")
            else:
                log_to_file("[Ablation] query_rewrite disabled")

            log_to_file("[Action - Tool Call]: entity_extract")
            ent_text = self._tool_entity_extract(db_schema, final_question, logger=log_to_file)
            log_to_file(f"[Observation - Result]: {ent_text}")
            entities = json_repair.loads(ent_text) if ent_text else []
            if not isinstance(entities, list):
                entities = []

            log_to_file("[Action - Tool Call]: apply_typed_masking")
            mask_res = self._tool_apply_typed_masking(final_question, entities, structured_db_schema)
            log_to_file(f"[Observation - Result]: {json.dumps(mask_res, ensure_ascii=False)[:3000]}")
            masked_question = mask_res.get("masked_query", "")
            masked_schema = mask_res.get("masked_schema_str", "")
            unmask_dict = mask_res.get("unmask_dict", {})
            masked_terms = mask_res.get("masked_terms", [])
            mask_mapping = mask_res.get("mask_mapping", {})

            log_to_file("[Action - Tool Call]: sql_generate")
            masked_sql_raw = self._tool_sql_generate(masked_question, masked_schema, logger=log_to_file)
            masked_sql = base.find_result(masked_sql_raw)
            log_to_file(f"[Observation - Result]: {masked_sql}")

            log_to_file("[Action - Tool Call]: sql_unmask")
            unmasked_sql = self._tool_sql_unmask(masked_sql, unmask_dict)
            log_to_file(f"[Observation - Result]: {unmasked_sql}")

            if not self.variant.disable_execution_check:
                log_to_file("[Action - Tool Call]: execute_sql_check")
                exec_obs = self._tool_execute_sql_check(unmasked_sql, db_path)
                log_to_file(f"[Observation - Result]: {exec_obs}")
            else:
                log_to_file("[Ablation] execute_sql_check disabled")

            return {
                "result_text": f"```sql\n{unmasked_sql}\n```",
                "final_question": final_question,
                "masked_question": masked_question,
                "masked_schema": masked_schema,
                "masked_sql": masked_sql,
                "unmask_dict": unmask_dict,
                "masked_terms": masked_terms,
                "mask_mapping": mask_mapping,
                "latency_sec": time.time() - start_time,
            }
        except Exception as e:
            log_to_file("[FixedPipeline] Error")
            log_to_file(traceback.format_exc())
            return {
                "result_text": json.dumps({"error": str(e), "type": type(e).__name__}, ensure_ascii=False),
                "final_question": final_question,
                "masked_question": masked_question,
                "masked_schema": masked_schema,
                "masked_sql": masked_sql,
                "unmask_dict": unmask_dict,
                "masked_terms": masked_terms,
                "mask_mapping": mask_mapping,
                "latency_sec": time.time() - start_time,
            }


def make_agent(variant: VariantConfig):
    if variant.fixed_pipeline:
        return FixedPipelineAgent(variant)
    return AblationPrivacyProtectionAgent(variant)


def process_single_item_extended(idx: int, item: Dict[str, Any], agent, total_num: int, variant_name: str, log_dir: str) -> Dict[str, Any]:
    case_id = str(item.get("question_id", f"idx_{idx}"))
    start_time = time.time()
    try:
        process_res = agent.process(item, case_id=case_id)
        result_text = process_res.get("result_text", "")
        pred_sql = base.find_result(result_text)
        latency = process_res.get("latency_sec", time.time() - start_time)
    except Exception as e:
        logging.error(f"[{variant_name}] case failed {case_id}: {type(e).__name__}: {e}")
        logging.error(traceback.format_exc())
        process_res = {
            "result_text": json.dumps({"error": str(e), "type": type(e).__name__}, ensure_ascii=False),
            "final_question": item.get("question", ""),
            "masked_question": "",
            "masked_schema": "",
            "masked_sql": "",
            "unmask_dict": {},
            "masked_terms": [],
            "mask_mapping": {},
        }
        pred_sql = ""
        latency = time.time() - start_time

    eval_original_idx = item.get("question_id")
    if eval_original_idx is None and item.get("masksql_idx"):
        s = str(item.get("masksql_idx"))
        if s.startswith("bird_"):
            eval_original_idx = s.replace("bird_", "", 1)
    if eval_original_idx is None:
        eval_original_idx = idx

    log_file = os.path.join(log_dir, f"case_{case_id}.txt")
    trace_metrics = parse_trace_metrics_from_log(log_file)

    return {
        "idx": idx,
        "eval_original_idx": str(eval_original_idx),
        "case_id": case_id,
        "db_id": item.get("db_id", ""),
        "question": item.get("question", ""),
        "evidence": item.get("evidence", ""),
        "final_question": process_res.get("final_question", item.get("question", "")),
        "masked_question": process_res.get("masked_question", ""),
        "masked_schema": process_res.get("masked_schema", ""),
        "masked_sql": process_res.get("masked_sql", ""),
        "unmask_dict": process_res.get("unmask_dict", {}),
        "masked_terms": process_res.get("masked_terms", []),
        "mask_mapping": process_res.get("mask_mapping", {}),
        "gold_sql": item.get("gold_sql", ""),
        "result_text": process_res.get("result_text", ""),
        "pred_sql": pred_sql,
        "masksql_idx": item.get("masksql_idx"),
        "masksql_match_method": item.get("masksql_match_method"),
        "orig_gold_schema_links": item.get("orig_gold_schema_links", {}),
        "orig_gold_value_links": item.get("orig_gold_value_links", {}),
        "annotated_links": item.get("annotated_links", {}),
        "latency_sec": latency,
        **trace_metrics,
    }


def run_variant_evaluation(
    variant: VariantConfig,
    paths: RuntimePaths,
    max_workers: int = 8,
    run_official_bird_eval: bool = True,
) -> Dict[str, Any]:
    logging.info(f"========== Running variant: {variant.name} ==========")
    variant_dir_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", variant.name)
    reasoning_file_dir = os.path.join(paths.output_dir, paths.inference_data_name, variant_dir_name)
    log_dir = os.path.join(reasoning_file_dir, "case_logs")
    os.makedirs(reasoning_file_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    # Required because the original code uses global variables in agent.process.
    base.log_dir = log_dir
    base.db_base_dir = paths.db_root_path

    data = base.read_data(paths.data_path)
    match_report = None
    if paths.masksql_test_path:
        data, match_report = base.filter_and_attach_masksql_annotations(
            data=data,
            masksql_test_path=paths.masksql_test_path,
            match_mode=paths.masksql_match_mode,
        )
        Path(os.path.join(reasoning_file_dir, "masksql_match_report.json")).write_text(
            json.dumps(match_report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logging.info(f"[{variant.name}] MaskSQL subset: {json.dumps(match_report, ensure_ascii=False)}")

    if not data:
        raise ValueError(f"[{variant.name}] no selected data rows")

    agent = make_agent(variant)
    results_list = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_item_extended, idx, item, agent, len(data), variant.name, log_dir): idx
            for idx, item in enumerate(data)
        }
        for fut in as_completed(futures):
            results_list.append(fut.result())

    results_list.sort(key=lambda x: x["idx"])

    for res in results_list:
        res.update(compute_exec_metrics_for_case(res, paths.db_root_path))
        if paths.masksql_test_path:
            pm = base.compute_privacy_metrics_for_result(res)
            pm = add_privacy_extra_metrics(pm)
            res.update({
                "MR": pm.get("MR"),
                "MR_present": pm.get("MR_present"),
                "prompt_privacy_score": pm.get("prompt_privacy_score"),
                "RI": pm.get("RI"),
                "Privacy-P": pm.get("Privacy-P"),
                "Privacy-R": pm.get("Privacy-R"),
                "Privacy-F1": pm.get("Privacy-F1"),
                "LeakRate": pm.get("LeakRate"),
                "prompt_direct_leak_count": pm.get("prompt_direct_leak_count"),
            })
        original_q = f"{res.get('question', '')} {res.get('evidence', '')}".strip()
        rewritten_or_masked = res.get("final_question") or res.get("masked_question") or ""
        res["SemSim_lexical"] = lexical_semantic_similarity(original_q, rewritten_or_masked)

    pred_sql_path = os.path.join(reasoning_file_dir, "predict.sql")
    predict_json_path = os.path.join(reasoning_file_dir, "predict.json")
    all_res_path = os.path.join(reasoning_file_dir, "all_res_extended.json")
    summary_path = os.path.join(reasoning_file_dir, "summary_metrics.json")

    bird_eval_dic = {}
    with open(pred_sql_path, "w", encoding="utf-8") as f_sql:
        for res in results_list:
            f_sql.write((res.get("pred_sql") or "") + "\n")
            eval_key = str(res.get("eval_original_idx", res["idx"]))
            bird_eval_dic[eval_key] = (res.get("pred_sql") or "") + "\t----- bird -----\t" + res.get("db_id", "")

    Path(predict_json_path).write_text(json.dumps(bird_eval_dic, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(all_res_path).write_text(json.dumps(results_list, ensure_ascii=False, indent=2), encoding="utf-8")

    privacy_report = None
    if paths.masksql_test_path:
        attack_client = None
        attack_model_name = ""
        if variant.run_ri_attack:
            if not (variant.ri_attack_base_url or variant.base_url):
                raise ValueError(f"[{variant.name}] run_ri_attack=True but no RI attack base_url is configured.")
            if not (variant.ri_attack_model_name or variant.model_name):
                raise ValueError(f"[{variant.name}] run_ri_attack=True but no RI attack model_name is configured.")
            attack_client = OpenAI(
                api_key=variant.ri_attack_api_key or variant.api_key or "EMPTY",
                base_url=variant.ri_attack_base_url or variant.base_url,
            )
            attack_model_name = variant.ri_attack_model_name or variant.model_name
            logging.info(
                f"[{variant.name}] RI attack enabled: "
                f"base_url={variant.ri_attack_base_url or variant.base_url}, model={attack_model_name}"
            )
        privacy_report = base.evaluate_privacy_results(
            results_list=results_list,
            output_path=os.path.join(reasoning_file_dir, "privacy_eval_masksql_style.json"),
            attack_client=attack_client,
            attack_model_name=attack_model_name,
        )

    summary = {
        "variant": variant.__dict__,
        "num_cases": len(results_list),
        "ExecAcc": safe_mean([r.get("exec_correct") for r in results_list]),
        "ValidSQL": safe_mean([r.get("valid_sql") for r in results_list]),
        "SemSim_lexical": safe_mean([r.get("SemSim_lexical") for r in results_list]),
        "AvgLatencySec": safe_mean([r.get("latency_sec") for r in results_list]),
        "AvgToolCalls": safe_mean([r.get("tool_calls") for r in results_list]),
        "AvgRounds": safe_mean([r.get("rounds") for r in results_list]),
        "AvgQueryRewriteCalls": safe_mean([r.get("query_rewrite_calls") for r in results_list]),
        "AvgQueryRestructureCalls": safe_mean([r.get("query_rewrite_calls") for r in results_list]),
        "AvgExecuteCheckCalls": safe_mean([r.get("execute_check_calls") for r in results_list]),
    }
    if paths.masksql_test_path:
        ri_value = None
        asr_value = None
        attack_recovered_count = None
        if privacy_report is not None:
            aggregate = privacy_report.get("aggregate", {})
            ri_value = aggregate.get("RI")
            if ri_value is not None:
                asr_value = 1.0 - float(ri_value)
            attack_recovered_count = sum(row.get("attack_recovered_count", 0) for row in privacy_report.get("per_case", []))

        summary.update({
            "MR": safe_mean([r.get("MR") for r in results_list]),
            "MR_present": safe_mean([r.get("MR_present") for r in results_list]),
            "Privacy-P": safe_mean([r.get("Privacy-P") for r in results_list]),
            "Privacy-R": safe_mean([r.get("Privacy-R") for r in results_list]),
            "Privacy-F1": safe_mean([r.get("Privacy-F1") for r in results_list]),
            "LeakRate": safe_mean([r.get("LeakRate") for r in results_list]),
            "PromptPrivacyScore": safe_mean([r.get("prompt_privacy_score") for r in results_list]),
            "RI": ri_value,
            "ASR": asr_value,
            "AttackRecoveredCount": attack_recovered_count,
            "RIAttackEnabled": bool(variant.run_ri_attack),
            "RIAttackModel": variant.ri_attack_model_name or variant.model_name if variant.run_ri_attack else None,
        })

    Path(summary_path).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info(f"[{variant.name}] Summary: {json.dumps(summary, ensure_ascii=False)}")

    if run_official_bird_eval:
        try:
            base.eval_exec(
                predicted_sql_path=predict_json_path,
                db_root_path=paths.db_root_path,
                data_mode=paths.data_mode,
                ground_truth_path=paths.ground_truth_path,
                num_cpus=8,
                mode_gt="gt",
                mode_predict="gpt",
                diff_json_path=paths.diff_json_path,
                meta_time_out=30,
                output_dir=reasoning_file_dir,
                inference_data_name=paths.inference_data_name,
            )
        except Exception as e:
            logging.warning(f"[{variant.name}] official eval failed: {e}")

    return summary



def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)) and value is not None:
        return float(value)
    return None


def _metric_values(summaries: List[Dict[str, Any]], key: str) -> List[float]:
    vals = []
    for s in summaries:
        v = _to_float(s.get(key))
        if v is not None:
            vals.append(v)
    return vals


def _mean_std(vals: List[float]) -> Tuple[Optional[float], Optional[float]]:
    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[0], 0.0
    return float(mean(vals)), float(stdev(vals))


def _as_percent(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return float(value) * 100.0


def _fmt_pct(value: Optional[float]) -> str:
    return "" if value is None else f"{value * 100.0:.2f}"


def _fmt_mean_std_pct(mean_value: Optional[float], std_value: Optional[float]) -> str:
    if mean_value is None:
        return "--"
    if std_value is None:
        std_value = 0.0
    return f"{mean_value * 100.0:.2f} $\\pm$ {std_value * 100.0:.2f}"


def save_summary_table(summaries: List[Dict[str, Any]], out_csv: str):
    """Save per-run raw summaries. Values are kept in ratio form except *_pct helper columns."""
    if not summaries:
        return
    keys = [
        "run_id", "base_variant_name", "num_cases",
        "ExecAcc", "Privacy-R", "Privacy-P", "Privacy-F1",
        "MR", "MR_present", "ValidSQL", "LeakRate", "PromptPrivacyScore",
        "RI", "ASR", "AttackRecoveredCount", "RIAttackEnabled", "RIAttackModel",
        "SemSim_lexical", "AvgToolCalls", "AvgRounds", "AvgLatencySec",
        "AvgQueryRewriteCalls", "AvgQueryRestructureCalls", "AvgExecuteCheckCalls"
    ]
    pct_keys = ["ExecAcc", "Privacy-R", "Privacy-P", "Privacy-F1", "MR", "MR_present"]
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["variant"] + keys + [f"{k}_pct" for k in pct_keys])
        for s in summaries:
            row = [s.get("variant", {}).get("name", "")] + [s.get(k) for k in keys]
            row += [_fmt_pct(_to_float(s.get(k))) for k in pct_keys]
            writer.writerow(row)


def aggregate_repeated_summaries(
    summaries: List[Dict[str, Any]],
    metrics: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Aggregate repeated runs by the original variant name."""
    if metrics is None:
        metrics = ["ExecAcc", "Privacy-R", "Privacy-P", "Privacy-F1"]

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    order: List[str] = []
    for s in summaries:
        base_name = s.get("base_variant_name") or s.get("variant", {}).get("name", "")
        if base_name not in grouped:
            grouped[base_name] = []
            order.append(base_name)
        grouped[base_name].append(s)

    rows: List[Dict[str, Any]] = []
    for base_name in order:
        group = grouped[base_name]
        row: Dict[str, Any] = {
            "variant": base_name,
            "num_runs": len(group),
            "num_cases": group[0].get("num_cases") if group else None,
        }
        for metric in metrics:
            vals = _metric_values(group, metric)
            m, sd = _mean_std(vals)
            row[f"{metric}_mean"] = m
            row[f"{metric}_std"] = sd
            row[f"{metric}_mean_pct"] = _as_percent(m)
            row[f"{metric}_std_pct"] = _as_percent(sd)
            row[f"{metric}_mean_std_pct"] = _fmt_mean_std_pct(m, sd)
        rows.append(row)
    return rows


def save_aggregate_table(rows: List[Dict[str, Any]], out_csv: str, metrics: Optional[List[str]] = None):
    if not rows:
        return
    if metrics is None:
        metrics = ["ExecAcc", "Privacy-R", "Privacy-P", "Privacy-F1"]
    header = ["variant", "num_runs", "num_cases"]
    for metric in metrics:
        header.extend([f"{metric}_mean_pct", f"{metric}_std_pct", f"{metric}_mean_std_pct"])
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            writer.writerow([row.get(k) for k in header])


def save_latex_ablation_table(rows: List[Dict[str, Any]], out_tex: str):
    """Save a compact Overleaf-ready table with mean ± std in percentage points."""
    name_map = {
        "PrivAgent-R1_full": "\\textsc{PrivAgent-R1}",
        "w_o_agentic_refinement_fixed_pipeline": "w/o Agentic Refinement",
        "w_o_query_restructuring": "w/o Query Restructuring",
        "w_o_execution_check": "w/o Execution Check",
    }
    lines = []
    lines.append(r"\begin{table}[t]")
    lines.append(r"\centering")
    lines.append(r"\small")
    lines.append(r"\begin{tabular}{lcccc}")
    lines.append(r"\toprule")
    lines.append(r"Variant & EX $\uparrow$ & M-R $\uparrow$ & M-P $\uparrow$ & M-F1 $\uparrow$ \\")
    lines.append(r"\midrule")
    for row in rows:
        raw_name = row.get("variant", "")
        name = name_map.get(raw_name, raw_name.replace("_", r"\_"))
        ex = row.get("ExecAcc_mean_std_pct", "--")
        mr = row.get("Privacy-R_mean_std_pct", "--")
        mp = row.get("Privacy-P_mean_std_pct", "--")
        mf1 = row.get("Privacy-F1_mean_std_pct", "--")
        lines.append(f"{name} & {ex} & {mr} & {mp} & {mf1} \\")
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\caption{Ablation results averaged over multiple runs. EX denotes execution accuracy. M-R, M-P, and M-F1 denote masking recall, masking precision, and masking F1, respectively.}")
    lines.append(r"\label{tab:ablation}")
    lines.append(r"\end{table}")
    Path(out_tex).write_text("\n".join(lines) + "\n", encoding="utf-8")

def load_config(path: str) -> Tuple[RuntimePaths, List[VariantConfig], Dict[str, Any]]:
    cfg = json.loads(Path(path).read_text(encoding="utf-8"))
    cfg = expand_env_vars(cfg)
    paths = RuntimePaths(**cfg["paths"])
    options = cfg.get("options", {})

    # Allow global RI-attack settings under options so you do not need to repeat
    # the same attack model configuration for every variant. Variant-level fields
    # still take precedence.
    global_variant_defaults = {}
    for key in ["run_ri_attack", "ri_attack_base_url", "ri_attack_model_name", "ri_attack_api_key", "temperature"]:
        if key in options:
            global_variant_defaults[key] = options[key]

    variants = []
    for raw_v in cfg["variants"]:
        v = dict(global_variant_defaults)
        v.update(raw_v)

        # Naming aliases: paper uses query restructuring, but the implementation
        # still disables the old query_rewrite tool internally.
        if "disable_query_restructuring" in v and "disable_query_rewrite" not in v:
            v["disable_query_rewrite"] = v.pop("disable_query_restructuring")
        else:
            v.pop("disable_query_restructuring", None)

        variants.append(VariantConfig(**v))

    return paths, variants, options



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="JSON config file")
    parser.add_argument("--num-runs", type=int, default=None, help="Override options.num_runs/repeats")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    paths, variants, options = load_config(args.config)
    max_workers = int(options.get("max_workers", 8))
    run_official_bird_eval = bool(options.get("run_official_bird_eval", True))
    num_runs = int(args.num_runs or options.get("num_runs", options.get("repeats", 1)))
    metrics = options.get("aggregate_metrics", ["ExecAcc", "Privacy-R", "Privacy-P", "Privacy-F1"])

    all_run_summaries: List[Dict[str, Any]] = []
    for run_id in range(1, num_runs + 1):
        logging.info(f"========== Repeated run {run_id}/{num_runs} ==========")
        for variant in variants:
            run_variant = copy.deepcopy(variant)
            base_variant_name = variant.name
            run_variant.name = f"{base_variant_name}__run{run_id}"
            summary = run_variant_evaluation(
                variant=run_variant,
                paths=paths,
                max_workers=max_workers,
                run_official_bird_eval=run_official_bird_eval,
            )
            # Keep the original variant name for aggregation.
            summary["base_variant_name"] = base_variant_name
            summary["run_id"] = run_id
            all_run_summaries.append(summary)

    out_dir = os.path.join(paths.output_dir, paths.inference_data_name)
    os.makedirs(out_dir, exist_ok=True)

    raw_csv = os.path.join(out_dir, "experiment_summary_runs.csv")
    save_summary_table(all_run_summaries, raw_csv)
    logging.info(f"Saved per-run summary table to {raw_csv}")

    aggregate_rows = aggregate_repeated_summaries(all_run_summaries, metrics=metrics)
    agg_csv = os.path.join(out_dir, "experiment_summary_mean_std.csv")
    save_aggregate_table(aggregate_rows, agg_csv, metrics=metrics)
    logging.info(f"Saved mean/std summary table to {agg_csv}")

    tex_path = os.path.join(out_dir, "ablation_table_mean_std.tex")
    save_latex_ablation_table(aggregate_rows, tex_path)
    logging.info(f"Saved Overleaf-ready ablation table to {tex_path}")


if __name__ == "__main__":
    main()
