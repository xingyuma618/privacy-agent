
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[2]))

import os
import json
import logging
from openai import OpenAI
from typing import Dict, List, Any, Tuple, Optional, Set
import re
import copy
import sqlite3
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed 
# from func_timeout import func_timeout, FunctionTimedOut # 已弃用，使用原生机制替代
import json_repair  

# Import your existing template files
from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template
from tools import generate_schema_string_from_masked, execute_sql,  find_result, safe_execute_sql

# ==========================================
# 引入各个步骤的示例数据
# ==========================================
from examples.evaluate_mask_en import evaluate_mask_messages
from examples.evaluate_structure_en import evaluate_structure_messages

# Configure logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_data(path, encoding="utf-8"):
    if path.endswith('.jsonl'):
        with open(path, "r", encoding=encoding) as f:
            data = [json.loads(line) for line in f if line.strip()]
    else:
        with open(path, "r", encoding=encoding) as f:
            data = json.load(f)
            
    sorted_data = sorted(data, key=lambda x: int(x.get('question_id', 0)))
    return sorted_data


# ==========================================
# MaskSQL-style Privacy Evaluation Utilities
# ==========================================
MASK_TOKEN_PATTERN = re.compile(
    r"(?:\btable_\d+\b|\bcol_\d+\b|\bval_\d+\b|\[[TCV]\d+\])",
    re.IGNORECASE,
)


def _normalize_text_for_key(text: Any) -> str:
    """Normalize NL text for robust dataset alignment."""
    if text is None:
        return ""
    text = str(text)
    text = text.replace("\u00a0", " ")
    text = text.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def _normalize_sql_for_key(sql: Any) -> str:
    """Normalize SQL for robust dataset alignment without changing semantics."""
    if sql is None:
        return ""
    sql = str(sql)
    sql = sql.replace("\u00a0", " ")
    sql = sql.replace("`", "")
    sql = re.sub(r"\s+", " ", sql).strip().rstrip(";")
    return sql.lower()


def _normalize_for_exact_match(text: Any) -> str:
    if text is None:
        return ""
    text = str(text)
    text = text.replace("\u00a0", " ")
    text = text.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _contains_surface(text: Any, surface: Any) -> bool:
    """Exact-ish surface matching with alnum boundaries to avoid matching 6 in 2016."""
    text_norm = _normalize_for_exact_match(text)
    surface_norm = _normalize_for_exact_match(surface)
    if not text_norm or not surface_norm:
        return False
    pattern = r"(?<![A-Za-z0-9_])" + re.escape(surface_norm) + r"(?![A-Za-z0-9_])"
    return re.search(pattern, text_norm, flags=re.IGNORECASE) is not None


def _parse_schema_link_type(link_value: Any) -> str:
    link_text = str(link_value or "").strip().upper()
    if link_text.startswith("TABLE:"):
        return "table"
    if link_text.startswith("COLUMN:"):
        return "column"
    return "schema"


def _extract_mask_tokens(text: Any) -> List[str]:
    if not isinstance(text, str):
        return []
    tokens = MASK_TOKEN_PATTERN.findall(text)
    # Preserve first occurrence order while removing duplicates.
    seen: Set[str] = set()
    uniq = []
    for tok in tokens:
        key = tok.lower()
        if key not in seen:
            seen.add(key)
            uniq.append(tok)
    return uniq


def _make_masksql_keys(record: Dict[str, Any], source: str) -> List[Tuple[str, str, str]]:
    """
    Build candidate alignment keys.

    source='bird' uses: db_id, question/evidence, gold_sql.
    source='masksql' uses: orig_db_id, orig_question/orig_evidence, orig_gold_sql.
    """
    if source == "bird":
        db_id = record.get("db_id", "")
        question = record.get("question", "") or ""
        evidence = record.get("evidence", "") or ""
        sql = record.get("gold_sql", "")
    elif source == "masksql":
        db_id = record.get("orig_db_id", record.get("db_id", ""))
        question = record.get("orig_question", record.get("question", "")) or ""
        evidence = record.get("orig_evidence", record.get("evidence", "")) or ""
        sql = record.get("orig_gold_sql", record.get("gold_sql", ""))
    else:
        raise ValueError(f"Unknown source: {source}")

    question_candidates = [question]
    if evidence:
        question_candidates.append(f"{question} {evidence}")
    question_candidates.append(f"{question} {evidence}".strip())

    keys = []
    for q in question_candidates:
        keys.append((str(db_id), _normalize_text_for_key(q), _normalize_sql_for_key(sql)))
    # De-duplicate while preserving order.
    deduped = []
    seen = set()
    for k in keys:
        if k not in seen:
            seen.add(k)
            deduped.append(k)
    return deduped


def load_masksql_annotation_index(masksql_test_path: str) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    """Load all_chunks.json / masksql_test.json and index it by db_id + question/evidence + gold_sql."""
    with open(masksql_test_path, "r", encoding="utf-8") as f:
        masksql_rows = json.load(f)

    index: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    duplicate_keys = 0
    for row in masksql_rows:
        for key in _make_masksql_keys(row, source="masksql"):
            if key in index:
                duplicate_keys += 1
            index[key] = row
    if duplicate_keys:
        logging.warning(f"MaskSQL annotation index has {duplicate_keys} duplicate normalized keys; later rows overwrote earlier rows.")
    return index


def select_masksql_annotated_subset(
    bird_data: List[Dict[str, Any]],
    masksql_test_path: str,
    unmatched_save_path: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Select BIRD examples that also appear in the MaskSQL annotation file.

    Matched items are deep-copied and enriched with `_masksql_annotation` containing:
      - masksql_idx
      - orig_gold_schema_links
      - orig_gold_value_links
      - orig_masked
    """
    ann_index = load_masksql_annotation_index(masksql_test_path)
    matched: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []

    for item in bird_data:
        ann = None
        for key in _make_masksql_keys(item, source="bird"):
            ann = ann_index.get(key)
            if ann is not None:
                break
        if ann is None:
            unmatched.append({
                "question_id": item.get("question_id"),
                "db_id": item.get("db_id"),
                "question": item.get("question", ""),
                "evidence": item.get("evidence", ""),
                "gold_sql": item.get("gold_sql", ""),
            })
            continue

        new_item = copy.deepcopy(item)
        new_item["_masksql_annotation"] = {
            "masksql_idx": ann.get("idx"),
            "orig_db_id": ann.get("orig_db_id"),
            "orig_question": ann.get("orig_question", ""),
            "orig_evidence": ann.get("orig_evidence", ""),
            "orig_gold_sql": ann.get("orig_gold_sql", ""),
            "orig_masked": ann.get("orig_masked", ""),
            "orig_gold_schema_links": ann.get("orig_gold_schema_links", {}) or {},
            "orig_gold_value_links": ann.get("orig_gold_value_links", {}) or {},
        }
        matched.append(new_item)

    if unmatched_save_path:
        os.makedirs(os.path.dirname(unmatched_save_path), exist_ok=True)
        with open(unmatched_save_path, "w", encoding="utf-8") as f:
            json.dump(unmatched, f, ensure_ascii=False, indent=2)

    stats = {
        "bird_total": len(bird_data),
        "masksql_index_keys": len(ann_index),
        "matched_total": len(matched),
        "unmatched_total": len(unmatched),
        "match_rate_over_bird": len(matched) / len(bird_data) if bird_data else 0.0,
    }
    return matched, stats


def _gold_sensitive_items(annotation: Dict[str, Any]) -> List[Dict[str, str]]:
    """Convert MaskSQL human links into a flat list of gold sensitive mentions."""
    items: List[Dict[str, str]] = []
    schema_links = annotation.get("orig_gold_schema_links", {}) or {}
    value_links = annotation.get("orig_gold_value_links", {}) or {}

    for mention, target in schema_links.items():
        if mention is None or str(mention).strip() == "":
            continue
        items.append({
            "mention": str(mention),
            "target": str(target),
            "kind": _parse_schema_link_type(target),
        })
    for mention, target in value_links.items():
        if mention is None or str(mention).strip() == "":
            continue
        items.append({
            "mention": str(mention),
            "target": str(target),
            "kind": "value",
        })

    # Deduplicate exact mention-target-kind triples.
    deduped: List[Dict[str, str]] = []
    seen = set()
    for item in items:
        key = (
            _normalize_for_exact_match(item["mention"]),
            _normalize_for_exact_match(item["target"]),
            item["kind"],
        )
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def evaluate_masking_recall(
    original_question_with_evidence: str,
    masked_question: str,
    annotation: Dict[str, Any],
    count_only_mentions_in_source: bool = True,
) -> Dict[str, Any]:
    """
    MaskSQL-style MR: fraction of human-annotated sensitive mentions that no longer appear in the masked NL question.

    `count_only_mentions_in_source=True` follows the paper's token-in-question definition and avoids
    penalizing normalized gold values that are not literally present in the question/evidence text.
    """
    gold_items = _gold_sensitive_items(annotation)
    eval_items: List[Dict[str, Any]] = []
    skipped_not_in_source: List[Dict[str, Any]] = []

    for item in gold_items:
        in_source = _contains_surface(original_question_with_evidence, item["mention"])
        if count_only_mentions_in_source and not in_source:
            skipped_not_in_source.append({**item, "reason": "gold mention not literally found in question/evidence"})
            continue

        still_visible = _contains_surface(masked_question, item["mention"])
        is_masked = not still_visible
        eval_items.append({
            **item,
            "in_source": in_source,
            "masked": is_masked,
            "still_visible": still_visible,
        })

    def _score_for(kind: Optional[str] = None) -> Tuple[int, int, float]:
        subset = [x for x in eval_items if kind is None or x["kind"] == kind]
        total = len(subset)
        masked = sum(1 for x in subset if x["masked"])
        return masked, total, masked / total if total else 1.0

    masked, total, mr = _score_for(None)
    schema_masked = sum(1 for x in eval_items if x["kind"] in {"table", "column", "schema"} and x["masked"])
    schema_total = sum(1 for x in eval_items if x["kind"] in {"table", "column", "schema"})
    value_masked, value_total, value_mr = _score_for("value")

    return {
        "masking_recall": mr,
        "masked_sensitive_count": masked,
        "gold_sensitive_count": total,
        "schema_masking_recall": schema_masked / schema_total if schema_total else 1.0,
        "schema_masked_count": schema_masked,
        "schema_gold_count": schema_total,
        "value_masking_recall": value_mr,
        "value_masked_count": value_masked,
        "value_gold_count": value_total,
        "raw_gold_sensitive_count": len(gold_items),
        "skipped_not_in_source_count": len(skipped_not_in_source),
        "unmasked_items": [x for x in eval_items if not x["masked"]],
        "skipped_not_in_source": skipped_not_in_source,
        "masked_question_mask_tokens": _extract_mask_tokens(masked_question),
    }


def _accepted_originals_for_mask(mask: str, unmask_dict: Dict[str, Any]) -> List[str]:
    if not isinstance(unmask_dict, dict):
        return []
    candidates = []
    for bucket in ["tables", "columns", "values"]:
        mapping = unmask_dict.get(bucket, {}) or {}
        for k, v in mapping.items():
            if str(k).lower() == str(mask).lower():
                val = str(v)
                candidates.append(val)
                if bucket == "columns" and "." in val:
                    candidates.append(val.split(".", 1)[1])
                break
    # Also allow bracketed MaskSQL-style tokens if provided by external results.
    return [c for c in candidates if c]


def _json_loads_best_effort(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        try:
            return json_repair.loads(text)
        except Exception:
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if match:
                return json_repair.loads(match.group(0))
            raise


def run_reidentification_attack(
    client: OpenAI,
    model_name: str,
    masked_question: str,
    masked_schema_str: str,
    unmask_dict: Dict[str, Any],
    temperature: float = 0,
) -> Dict[str, Any]:
    """
    Optional MaskSQL-style RI attack.

    The attacker sees only masked question/schema and predicts original tokens for masks in the NL question.
    RI = (# masks not exactly recovered) / (# masks in masked NL question).
    """
    masks = _extract_mask_tokens(masked_question)
    mask_originals = {m: _accepted_originals_for_mask(m, unmask_dict) for m in masks}
    masks = [m for m in masks if mask_originals.get(m)]

    if not masks:
        return {
            "reidentification_score": 1.0,
            "attack_total": 0,
            "attack_recovered": 0,
            "attack_guesses": {},
            "attack_details": [],
            "attack_error": "No question masks with known original values.",
        }

    prompt = f"""
You are evaluating privacy of a Text-to-SQL masking system.
Given a masked natural-language question and a masked database schema, infer the most likely original token for each mask token.
Return only a JSON object mapping each mask token to one guessed original string. If you cannot infer it, use an empty string.

Masked question:
{masked_question}

Masked schema:
{masked_schema_str}

Mask tokens to infer:
{json.dumps(masks, ensure_ascii=False)}
""".strip()

    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You infer masked database/query tokens. Output strict JSON only."},
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
        )
        raw_text = response.choices[0].message.content.strip()
        guesses = _json_loads_best_effort(raw_text)
        if not isinstance(guesses, dict):
            raise ValueError("RI attack output is not a JSON object")
    except Exception as e:
        return {
            "reidentification_score": None,
            "attack_total": len(masks),
            "attack_recovered": None,
            "attack_guesses": {},
            "attack_details": [],
            "attack_error": str(e),
        }

    details = []
    recovered = 0
    for mask in masks:
        guess = str(guesses.get(mask, ""))
        accepted = mask_originals.get(mask, [])
        is_recovered = any(_normalize_for_exact_match(guess) == _normalize_for_exact_match(x) for x in accepted)
        recovered += int(is_recovered)
        details.append({
            "mask": mask,
            "guess": guess,
            "accepted_originals": accepted,
            "recovered": is_recovered,
        })

    total = len(masks)
    return {
        "reidentification_score": 1.0 - recovered / total if total else 1.0,
        "attack_total": total,
        "attack_recovered": recovered,
        "attack_guesses": guesses,
        "attack_details": details,
        "attack_error": None,
    }


def evaluate_privacy_for_item(
    item: Dict[str, Any],
    privacy_trace: Dict[str, Any],
    agent: Optional["PrivacyProtectionAgent"] = None,
    enable_ri_attack: bool = False,
) -> Dict[str, Any]:
    annotation = item.get("_masksql_annotation", {}) or {}
    original_qe = f"{item.get('question', '')} {item.get('evidence', '')}".strip()
    masked_question = privacy_trace.get("masked_question", "") or ""
    masked_schema = privacy_trace.get("masked_schema", "") or ""
    unmask_dict = privacy_trace.get("unmask_dict", {}) or {}

    mr_result = evaluate_masking_recall(
        original_question_with_evidence=original_qe,
        masked_question=masked_question,
        annotation=annotation,
        count_only_mentions_in_source=True,
    )

    result = {
        **mr_result,
        "masksql_idx": annotation.get("masksql_idx"),
        "masked_question": masked_question,
        "masked_schema_available": bool(masked_schema),
        "ri_enabled": bool(enable_ri_attack),
        "reidentification_score": None,
    }

    if enable_ri_attack and agent is not None:
        ri_result = run_reidentification_attack(
            client=agent.client,
            model_name=agent.model_name,
            masked_question=masked_question,
            masked_schema_str=masked_schema,
            unmask_dict=unmask_dict,
            temperature=agent.temperature,
        )
        result.update(ri_result)

    return result


def summarize_privacy_results(results_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    privacy_rows = [r.get("privacy_eval", {}) for r in results_list if r.get("privacy_eval")]

    def _avg(field: str) -> Optional[float]:
        vals = [x.get(field) for x in privacy_rows if isinstance(x.get(field), (int, float))]
        return sum(vals) / len(vals) if vals else None

    def _sum(field: str) -> int:
        return int(sum(x.get(field, 0) or 0 for x in privacy_rows))

    total_gold = _sum("gold_sensitive_count")
    total_masked = _sum("masked_sensitive_count")
    total_schema = _sum("schema_gold_count")
    total_schema_masked = _sum("schema_masked_count")
    total_value = _sum("value_gold_count")
    total_value_masked = _sum("value_masked_count")

    ri_vals = [x.get("reidentification_score") for x in privacy_rows if isinstance(x.get("reidentification_score"), (int, float))]

    return {
        "evaluated_cases": len(privacy_rows),
        "macro_masking_recall": _avg("masking_recall"),
        "micro_masking_recall": total_masked / total_gold if total_gold else None,
        "macro_schema_masking_recall": _avg("schema_masking_recall"),
        "micro_schema_masking_recall": total_schema_masked / total_schema if total_schema else None,
        "macro_value_masking_recall": _avg("value_masking_recall"),
        "micro_value_masking_recall": total_value_masked / total_value if total_value else None,
        "macro_reidentification_score": sum(ri_vals) / len(ri_vals) if ri_vals else None,
        "total_gold_sensitive_mentions": total_gold,
        "total_masked_sensitive_mentions": total_masked,
        "total_unmasked_sensitive_mentions": total_gold - total_masked,
        "total_skipped_not_in_source": _sum("skipped_not_in_source_count"),
        "correct_cases": int(sum(1 for r in results_list if r.get("is_correct"))),
        "execution_accuracy_on_overlap": sum(1 for r in results_list if r.get("is_correct")) / len(results_list) if results_list else None,
    }
        
# ==========================================
# Agent Class
# ==========================================
class PrivacyProtectionAgent:
    def __init__(self, base_url: str, model_name: str, api_key: str = "EMPTY"):
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model_name = model_name
        self.temperature = 0

        self.structural_keys = {
            "columns", "column_name", "column_definition", "data_type", 
            "is_primary_key", "foreign_keys_info", "referenced_by", "examples", 
            "column_description", "value_description", "table_primary_keys", 
            "table_foreign_keys", "unique_constraints"
        }
        
        self.system_prompt = """
You are an NL2SQL privacy protection agent. You need to use the ReAct (Reasoning and Acting) paradigm to complete the privacy desensitization task for natural language queries. You must ensure that the desensitized natural language query still allows the model to write the correct SQL query.
Under this paradigm, you must alternate between Thought and Action, and after each action, proceed to the next step based on the received Observation, until reaching a final conclusion.

Sensitive Data Definition:
Any entity related to database tables, columns, or values present in the natural language query is treated as sensitive data.

[Processing Flow and Chain of Thought]
Please strictly follow these steps:
1. Evaluate Structure: Analyze the user's original Query to evaluate whether its privacy entities can be directly replaced. If a direct replacement destroys the syntactic structure (for example, replacing "How much does Alice earn per month?" directly into "How much dose [value_1] [column_1]" making it obvious that the relationship between [value_1] and [column_1] is lost), please call the `query_rewrite` tool to rewrite it.
2. Extract Entities: Using the finally decided Query (original or rewritten), call the `entity_extract` tool to extract all privacy entities.
3. Masking Operation: After obtaining the entity list, call the `apply_typed_masking` tool to mask the schema and the user's question respectively.
4. Evaluate Masking: Evaluate whether the masked query and schema are reasonable.
   - If the sentence structure is incomplete or differs significantly from the original question, making it unclear what the query is asking, it indicates that `query_rewrite` is needed to adjust the query sentence structure before masking again.
   - If there are unmasked entities, it means the entities extracted in the `entity_extract` step are incomplete. You need to directly supplement the missed entities you observed into `entities` and call `apply_typed_masking` again.
5. Generate SQL: Pass the masked query and schema to `sql_generate` to generate the desensitized SQL.
6. Unmask SQL: After getting the generated desensitized SQL, call the `sql_unmask` tool, passing in the SQL and the reverse mapping dictionary (`unmask_dict`) to restore the real SQL.
7. Execute SQL Check: Once you have the real unmasked SQL, call the `execute_sql_check` tool to verify if it is syntactically correct and can be successfully executed against the database.
8. Autonomous Semantic Evaluation & Repair: After receiving the execution result (Observation) from `execute_sql_check`, you MUST independently and autonomously evaluate the SQL.
   - Review Execution Errors: If the tool returns a syntax or execution error, analyze why it failed.
   - Review Semantic Logic: Even if the execution is "Success", check if the SQL accurately answers the Original User Question.
   - Autonomously Repair: If there are execution errors OR semantic flaws, you must autonomously REPAIR it in your mind. Write down your error analysis and correction plan in your `Thought`. You may call `execute_sql_check` again with your repaired SQL to verify it.
   - Once verified and semantically sound, proceed to final output.

[Output Requirements]
Before deciding to call any tool, you must first output your thought process in your answer! Each response should follow this logic:
Thought: [Write down your thought process here. Analyze the current input, the previous Observation, what next step you need to take, and why.]
Action: [Execute your action by triggering the corresponding function call]

After the tool executes, the system will return the result to you as an Observation.

When you have completed all steps and obtained the final correct real SQL (after your autonomous evaluation, execution check, and potential repair):
Thought: [State that you have successfully verified the unmasked SQL execution and semantic correctness. If it needed repair, explain what was fixed. State that the task is complete.]
Directly output the final real SQL as the final response, without calling any tools again. You need to place the final SQL in the ```sql\n\n``` markdown format.
"""
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "query_rewrite",
                    "description": "Call this tool when the original query formulation is irregular. If called multiple times, a rewrite instruction (rewrite_instruction) can be passed to correct previous errors.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query_to_rewrite": {
                                "type": "string",
                                "description": "The original query sentence to be rewritten"
                            },
                            "rewrite_instruction": {
                                "type": "string",
                                "description": "(Optional) Additional rewrite requirements or instructions."
                            }
                        },
                        "required": ["query_to_rewrite"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "entity_extract",
                    "description": "Extract the Schema Linking entity information array.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query_to_extract": {"type": "string"}
                        },
                        "required": ["query_to_extract"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "apply_typed_masking",
                    "description": "Simultaneously perform co-frequency table, column, and value masking replacement on the Query and Structured Schema at the underlying level.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "entities": {
                                "type": "array", 
                                "items": {"type": "object"}
                            }
                        },
                        "required": ["query", "entities"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "sql_generate",
                    "description": "Generate the final SQL statement based on the masked query and masked Schema.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "masked_query": {
                                "type": "string",
                                "description": "The desensitized user question"
                            },
                            "masked_schema_str": {
                                "type": "string",
                                "description": "The desensitized database Schema string"
                            }
                        },
                        "required": ["masked_query", "masked_schema_str"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "sql_unmask",
                    "description": "Restore the masked SQL to the real SQL statement.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "masked_sql": {
                                "type": "string",
                                "description": "The desensitized SQL generated by sql_generate"
                            },
                            "unmask_dict": {
                                "type": "object",
                                "description": "The reverse mapping dictionary"
                            }
                        },
                        "required": ["masked_sql", "unmask_dict"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "execute_sql_check",
                    "description": "Check if the unmasked SQL can be executed successfully in the database. Returns 'Success' or an execution error message to help you repair the SQL.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "unmasked_sql": {
                                "type": "string",
                                "description": "The real SQL statement to check execution"
                            }
                        },
                        "required": ["unmasked_sql"]
                    }
                }
            }
        ]


    def _tool_query_rewrite(self, db_schema: str, query_to_rewrite: str, rewrite_instruction: str = "", logger=None) -> str:
        rewrite_client = OpenAI(api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"), base_url="http://127.0.0.1:2335/v1")
        model_name = str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-50')
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_rewrite}"
        if rewrite_instruction:
            input_str += f"\n- Rewrite Instruction:\n{rewrite_instruction}"
        messages = get_query_rewrite_messages(input_str)
        response = rewrite_client.chat.completions.create(model=model_name, messages=messages, temperature=self.temperature)
        ans = response.choices[0].message.content.strip()
        return ans

    def _tool_entity_extract(self, db_schema: str, query_to_extract: str, logger=None) -> str:
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_extract}"
        messages = get_entity_extract_template(input_str)
        entity_extract_client = OpenAI(api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"), base_url="http://127.0.0.1:2336/v1")
        model_name = str(_RELEASE_ROOT / 'LlamaFactory/saves/entity_extract_v1/checkpoint-25')
        response = entity_extract_client.chat.completions.create(model=model_name, messages=messages, temperature=self.temperature)
        response_text = response.choices[0].message.content.strip()
        match = re.search(r'\[\s*\{.*?\}\s*\]', response_text, re.DOTALL)
        return match.group(0) if match else "[]"
    
    def _tool_sql_generate(self, masked_query, masked_schema_str, logger=None):
        input_str = f"- Database Schema:\n{masked_schema_str}\n- User's Question:\n{masked_query}"
        messages = get_sql_generate_template(input_str)
        response = self.client.chat.completions.create(model=self.model_name, messages=messages, temperature=self.temperature)
        return response.choices[0].message.content.strip()

    def _tool_sql_unmask(self, masked_sql: str, unmask_dict: Any) -> str:
        if isinstance(unmask_dict, str):
            try: unmask_dict = json_repair.loads(unmask_dict)
            except Exception: pass
        if not masked_sql or not isinstance(masked_sql, str) or not isinstance(unmask_dict, dict):
            return masked_sql
        mask_info_list = []
        for category in ["tables", "columns", "values"]:
            if category in unmask_dict:
                for mask, orig in unmask_dict[category].items():
                    mask_info_list.append({"mask": mask, "original": str(orig), "category": category})
        mask_info_list.sort(key=lambda x: len(x["mask"]), reverse=True)
        unmasked_text = masked_sql
        for info in mask_info_list:
            mask, original_value, category = info["mask"], info["original"], info["category"]
            pattern = re.compile(r'\b' + re.escape(mask) + r'\b', re.IGNORECASE)
            def replace_match(match):
                text = match.string
                start, end = match.start(), match.end()
                left_char = text[start - 1] if start > 0 else ""
                right_char = text[end] if end < len(text) else ""
                val = original_value
                if category == "columns" and "." in val:
                    tbl_part, col_part = val.split(".", 1)
                    if left_char == ".":
                        val = col_part
                    else:
                        res = f"`{tbl_part}`.`{col_part}`"
                        if left_char not in ("`", "'", '"') and right_char not in ("`", "'", '"'): return res
                        return f"{tbl_part}.{col_part}"
                if category in ["tables", "columns"]:
                    if left_char not in ("`", "'", '"') and right_char not in ("`", "'", '"'): return f"`{val}`"
                return val
            unmasked_text = pattern.sub(replace_match, unmasked_text)
        return unmasked_text

    def _tool_execute_sql_check(self, unmasked_sql: str, db_path: str) -> str:
        if not unmasked_sql or not db_path:
            return "Error: Missing SQL or Database Path."
        
        try:
            conn = sqlite3.connect(db_path)
            check_sql = f"SELECT * FROM ({unmasked_sql.strip().rstrip(';')}) LIMIT 1;"
            safe_execute_sql(conn, check_sql, timeout_sec=15.0)
            return "Success"
        except TimeoutError:
            return "SQL Execution Error: Execution timed out (exceeded 15 seconds). The query might be too complex or have an infinite loop (e.g., Cartesian product)."
        except Exception as e:
            return f"SQL Execution Error: {str(e)}"
        finally:
            if 'conn' in locals():
                conn.close()

    def _tool_apply_typed_masking(self, query, entities, structured_db_schema):
        table_map, col_map, val_map = {}, {}, {}
        t_idx, c_idx, v_idx = 1, 1, 1
        
        for table_name, table_info in structured_db_schema.items():
            if not table_name or not str(table_name).strip(): continue 
            if table_name not in table_map:
                table_map[table_name] = f"table_{t_idx}"
                t_idx += 1
            cols = table_info["columns"] if "columns" in table_info else [{"column_name": k, **v} for k, v in table_info.items()]
            for col_info in cols:
                col_name = col_info.get("column_name", "")
                if not col_name or not str(col_name).strip(): continue 
                if (table_name, col_name) not in col_map:
                    col_map[(table_name, col_name)] = f"col_{c_idx}"
                    c_idx += 1

        for ent in entities:
            if ent.get("entity_type") == "cell_value" and ent.get("cell_value") is not None:
                val_str = str(ent["cell_value"])
                if not val_str.strip(): continue
                if val_str not in val_map:
                    val_map[val_str] = f"val_{v_idx}"
                    v_idx += 1

        def replace_in_string(text):
            if not isinstance(text, str) or not text: return text
            replace_dict = {orig: mask for orig, mask in table_map.items()}
            replace_dict.update({orig_c: mask for (orig_t, orig_c), mask in col_map.items()})
            replace_dict.update({orig: mask for orig, mask in val_map.items()})
            sorted_keys = sorted(replace_dict.keys(), key=lambda k: len(str(k)), reverse=True)
            res = text
            for k in sorted_keys:
                k_str = str(k)
                if not k_str: continue
                if re.match(r'^\w+$', k_str):
                    res = re.sub(r'\b' + re.escape(k_str) + r'\b', replace_dict[k], res, flags=re.IGNORECASE)
                else:
                    res = res.replace(k_str, replace_dict[k])
            return res

        masked_query = query
        sorted_entities = sorted(entities, key=lambda x: len(str(x.get("entity", ""))), reverse=True)
        for ent in sorted_entities:
            ent_str = str(ent.get("entity", ""))
            if not ent_str.strip(): continue
            mask = None
            if ent.get("entity_type") == "table": mask = table_map.get(ent.get("table"))
            elif ent.get("entity_type") == "column": mask = col_map.get((ent.get("table"), ent.get("column")))
            elif ent.get("entity_type") == "cell_value": mask = val_map.get(str(ent.get("cell_value")))
            if mask:
                pattern = re.compile(r'\b' + re.escape(ent_str) + r'\b', re.IGNORECASE) if re.match(r'^\w+$', ent_str) else re.compile(re.escape(ent_str), re.IGNORECASE)
                masked_query = pattern.sub(mask, masked_query)
        masked_query = replace_in_string(masked_query)

        masked_schema = {}
        for table_name, table_info in structured_db_schema.items():
            if not table_name or not str(table_name).strip(): continue
            m_table_name = table_map.get(table_name, table_name)
            m_table_info = {}
            if "columns" in table_info:
                m_table_info["columns"] = []
                for col_info in table_info["columns"]:
                    m_col_info = {}
                    for attr_key, attr_val in col_info.items():
                        if attr_key == "examples": continue 
                        m_col_info[attr_key] = replace_in_string(attr_val) if isinstance(attr_val, str) else attr_val
                    orig_col_name = col_info.get("column_name", "")
                    m_col_info["column_name"] = col_map.get((table_name, orig_col_name), orig_col_name)
                    m_table_info["columns"].append(m_col_info)
                for k, v in table_info.items():
                    if k not in ["columns", "examples"]: 
                        m_table_info[k] = [replace_in_string(item) if isinstance(item, str) else item for item in v] if isinstance(v, list) else replace_in_string(v) if isinstance(v, str) else v
            else:
                for col_name, col_attrs in table_info.items():
                    m_col_name = col_map.get((table_name, col_name), col_name)
                    m_col_attrs = {}
                    for attr_key, attr_val in col_attrs.items():
                        if attr_key == "examples": continue
                        m_col_attrs[attr_key] = replace_in_string(attr_val) if isinstance(attr_val, str) else attr_val
                    m_table_info[m_col_name] = m_col_attrs
            masked_schema[m_table_name] = m_table_info

        unmask_dict = {
            "tables": {mask: orig for orig, mask in table_map.items()},
            "columns": {mask: f"{orig_t}.{orig_c}" for (orig_t, orig_c), mask in col_map.items()},
            "values": {mask: orig for orig, mask in val_map.items()}
        }
        masked_schema_str = generate_schema_string_from_masked(masked_schema)
        return {"masked_query": masked_query, "masked_schema_str": masked_schema_str, "unmask_dict": unmask_dict}

    def process(self, item: Dict[str, Any], case_id: str = "default", db_path: str = "") -> Tuple[str, List[Dict[str, Any]], bool, Dict[str, Any]]:
        log_dir = "case_logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f"case_{case_id}.txt")
        if os.path.exists(log_file_path): open(log_file_path, 'w', encoding='utf-8').close()

        def log_to_file(text: str):
            with open(log_file_path, "a", encoding="utf-8") as f: f.write(text + "\n")

        original_question = item['question']
        db_schema = item.get('db_schema', '') 
        evidence = item.get("evidence", '')
        structured_db_schema = item.get('structured_db_schema', {}) 
        
        user_prompt = f"- Database Schema:\n{db_schema}\n\n- User Question:\n{original_question} {evidence}\n\nTask started. Please provide your analysis (Thought) and call the tools (Action)."

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        training_messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        masked_schema, masked_sql, unmask_dict, masked_question = '', '', '', ''
        last_masking_result: Dict[str, Any] = {}
        has_tool_error = False
        
        for iteration in range(20):
            log_to_file(f"\n{'='*20} [Round {iteration + 1}] {'='*20}")
            
            # =========================================================================
            # 动态注入 Context 逻辑 (精准限定触发阶段)
            # =========================================================================
            dynamic_examples = []
            
            if iteration == 0:
                # 阶段 1: 仅在初始评估结构阶段 (Evaluate Structure) 加入对应的示例
                dynamic_examples = evaluate_structure_messages
            elif len(messages) > 0:
                # 获取紧接着的最后一条消息记录
                last_msg = messages[-1]
                # 兼容 openai 原生对象和本地字典格式
                last_role = last_msg.get("role") if isinstance(last_msg, dict) else getattr(last_msg, "role", None)
                last_name = last_msg.get("name") if isinstance(last_msg, dict) else getattr(last_msg, "name", None)
                
                # 阶段 4: 仅当上一轮操作是刚刚完成 apply_typed_masking 时，此时进入 Evaluate Masking 阶段
                if last_role == "tool" and last_name == "apply_typed_masking":
                    dynamic_examples = evaluate_mask_messages
                else:
                    # 其他任何阶段（如 sql生成、修复等）坚决不加入这些示例，防止 prompt 过长污染
                    dynamic_examples = []

            # 组装发给大模型的最终报文
            current_api_messages = [messages[0]] + dynamic_examples + messages[1:]
            
            # =========================================================================
            # 详细打印组装后的最终输入报文到 case_logs 中
            # =========================================================================
            log_to_file(f"\n👇 [MODEL INPUT - Messages sent to API in Round {iteration + 1}] 👇")
            for msg_idx, msg_obj in enumerate(current_api_messages):
                # 兼容 openai 库返回的对象格式 (ChatCompletionMessage) 或 本地维护的字典格式 (dict)
                role = msg_obj.get("role") if isinstance(msg_obj, dict) else msg_obj.role
                content = msg_obj.get("content") if isinstance(msg_obj, dict) else msg_obj.content
                name = msg_obj.get("name") if isinstance(msg_obj, dict) else getattr(msg_obj, "name", None)
                
                role_tag = f"[{role.upper()}]"
                if name:
                    role_tag += f" (Name: {name})"
                
                log_to_file(f"{role_tag}:")
                if content:
                    log_to_file(f"{content}")
                    
                tool_calls = msg_obj.get("tool_calls") if isinstance(msg_obj, dict) else getattr(msg_obj, "tool_calls", None)
                if tool_calls:
                    try:
                        # 尝试将对象安全地解析为字符串便于观察
                        tc_list = []
                        for tc in tool_calls:
                            tc_id = tc.get('id') if isinstance(tc, dict) else tc.id
                            func_name = tc['function'].get('name') if isinstance(tc, dict) else tc.function.name
                            func_args = tc['function'].get('arguments') if isinstance(tc, dict) else tc.function.arguments
                            tc_list.append({"id": tc_id, "type": "function", "function": {"name": func_name, "arguments": func_args}})
                        log_to_file(f"[TOOL_CALLS]: {json.dumps(tc_list, ensure_ascii=False, indent=2)}")
                    except Exception:
                        log_to_file(f"[TOOL_CALLS]: {tool_calls}")
                log_to_file("-" * 40)
            log_to_file(f"👆 [END OF MODEL INPUT] 👆\n")
            # =========================================================================

            # 发送请求
            response = self.client.chat.completions.create(
                model=self.model_name, 
                messages=current_api_messages, 
                tools=self.tools, 
                tool_choice="auto", 
                temperature=self.temperature,
                extra_body={"thinking": {"type": "disabled"}}
            )
            
            message = response.choices[0].message
            messages.append(message) 
            
            thought_text = message.content.strip() if message.content else "(Direct output, no text thought)"
            log_to_file(f"🧠 [Thought / Output]:\n{thought_text}\n")
            
            parsed_tool_calls = []
            if message.content:
                matches = re.finditer(r'<tool_call>\s*(.*?)\s*</tool_call>', message.content, re.DOTALL)
                for match_idx, match in enumerate(matches):
                    try:
                        tool_data = json_repair.loads(match.group(1))
                        parsed_tool_calls.append({"name": tool_data.get("name"), "arguments": tool_data.get("arguments", {}), "id": f"call_patch_{iteration}_{match_idx}"})
                        message.content = message.content.replace(match.group(0), "")
                    except json.JSONDecodeError: 
                        has_tool_error = True 
                        pass

            assistant_msg_for_train = {"role": "assistant", "content": message.content or ""}

            if message.tool_calls or parsed_tool_calls:
                try:
                    calls_to_process = [{"name": tc.function.name, "arguments": json_repair.loads(tc.function.arguments) if isinstance(tc.function.arguments, str) else tc.function.arguments, "id": tc.id} for tc in message.tool_calls] if message.tool_calls else parsed_tool_calls
                except Exception as e:
                    calls_to_process = []
                    has_tool_error = True

                assistant_msg_for_train["tool_calls"] = [
                    {
                        "id": tc["id"], "type": "function",
                        "function": {"name": tc["name"], "arguments": json.dumps(tc["arguments"], ensure_ascii=False) if isinstance(tc["arguments"], dict) else tc["arguments"]}
                    } for tc in calls_to_process
                ]
                training_messages.append(assistant_msg_for_train)

                for tc_data in calls_to_process:
                    func_name, args, tool_call_id = tc_data["name"], tc_data["arguments"], tc_data["id"]
                    log_args_str = json.dumps(args, ensure_ascii=False, indent=2)
                    
                    log_to_file(f"🛠️ [Action - Tool Call]: {func_name}")
                    log_to_file(f"   [Action - Arguments]: {log_args_str}\n")
                    
                    try:
                        if func_name == "query_rewrite": 
                            result = self._tool_query_rewrite(db_schema, args.get('query_to_rewrite', ''), args.get('rewrite_instruction', ''), logger=log_to_file)
                        elif func_name == "entity_extract": 
                            result = self._tool_entity_extract(db_schema, args.get('query_to_extract', ''), logger=log_to_file)
                        elif func_name == "apply_typed_masking": 
                            result = self._tool_apply_typed_masking(args.get('query', ''), args.get('entities', []), structured_db_schema)
                            if isinstance(result, dict):
                                last_masking_result = copy.deepcopy(result)
                                masked_question = result.get("masked_query", masked_question)
                                masked_schema = result.get("masked_schema_str", masked_schema)
                                unmask_dict = result.get("unmask_dict", unmask_dict)
                        elif func_name == "sql_generate":
                            masked_schema = args.get('masked_schema_str', '')
                            masked_question = args.get('masked_query', '')
                            result = self._tool_sql_generate(args.get('masked_query', ''), args.get('masked_schema_str', ''), logger=log_to_file)
                        elif func_name == "sql_unmask":
                            masked_sql = args.get('masked_sql', '')
                            unmask_dict = args.get('unmask_dict', {})
                            result = self._tool_sql_unmask(args.get('masked_sql', ''), unmask_dict)
                        elif func_name == "execute_sql_check":
                            # 让智能体调用工具检查错误，供它自行修复用
                            result = self._tool_execute_sql_check(args.get('unmasked_sql', ''), db_path)
                        else: 
                            result = f"Error: Unknown tool {func_name}"
                            has_tool_error = True 
                    except Exception as e:
                        result = f"Error executing {func_name}: {str(e)}"
                        has_tool_error = True 
                    
                    if isinstance(result, str) and (result.startswith("Error:") or result.startswith("Error executing")):
                        has_tool_error = True

                    log_result_str = str(result)
                    log_to_file(f"👀 [Observation - Result]:\n{log_result_str}\n")
                    
                    messages.append({"role": "tool", "tool_call_id": tool_call_id, "name": func_name, "content": str(result)})
                    training_messages.append({"role": "tool", "tool_call_id": tool_call_id, "name": func_name, "content": str(result)})

            else:
                training_messages.append(assistant_msg_for_train)
                                
                log_to_file(f"\n{'='*30} [CASE END] {'='*30}")
                log_to_file(f"[DB SCHEMA]:\n{db_schema}\n")
                log_to_file(f"[USER QUESTION]:\n{original_question}\n")
                log_to_file(f"[EVIDENCE]:\n{evidence}\n")
                log_to_file(f"✅ [GOLD SQL]:\n{item.get('gold_sql', '')}\n")
                log_to_file(f"=======================================================================")

                log_to_file(f"[MASKED DB SCHEMA]:\n{masked_schema}\n")
                log_to_file(f"[MASKED QUESTION]:\n{masked_question}\n")
                log_to_file(f"[MASKED SQL]:\n{masked_sql}\n")
                log_to_file(f"[MASKED DICT]:\n{unmask_dict}\n")

                log_to_file(f"✅ [Final Answer]:\n{message.content}\n")
                
                privacy_trace = {
                    "masked_question": masked_question,
                    "masked_schema": masked_schema,
                    "masked_sql": masked_sql,
                    "unmask_dict": unmask_dict,
                    "last_masking_result": last_masking_result,
                }
                return message.content, training_messages, has_tool_error, privacy_trace
        
        log_to_file(f"\n{'='*30} [CASE TIMEOUT] {'='*30}")
        return '{"error": "Agent timeout"}', [], True, {"error": "Agent timeout"}


# ==========================================
# Module: Independently extracted single-thread processing function
# ==========================================
def process_single_item(idx, item, agent: PrivacyProtectionAgent, total_num, db_base_dir, cache_dir, enable_ri_attack: bool = False):
    case_id = str(item.get("question_id", f"idx_{idx}"))
    db_id = item.get('db_id', 'unknown_db')
    gold_sql = item.get('gold_sql', '')
    
    cache_suffix = "_ri" if enable_ri_attack else ""
    cache_file = os.path.join(cache_dir, f"{case_id}{cache_suffix}.json")
    if os.path.exists(cache_file):
        logging.info(f"⏭️ Cache found for case [{idx+1}/{total_num}] - {case_id}, skipping execution.")
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    logging.info(f"⏳ Starting to process case [{idx+1}/{total_num}] - {case_id}")
    
    db_path = f"{db_base_dir}/{db_id}/{db_id}.sqlite"
    
    try:
        process_ret = agent.process(item, case_id=case_id, db_path=db_path)
        if len(process_ret) == 3:
            result_text, training_messages, has_tool_error = process_ret
            privacy_trace = {}
        else:
            result_text, training_messages, has_tool_error, privacy_trace = process_ret
        pred_sql = find_result(result_text)
        
        is_correct = 0
        if pred_sql and gold_sql:
            is_correct = execute_sql(pred_sql, gold_sql, db_path, logging, timeout_sec=20.0)
        
        if not is_correct or has_tool_error:
            training_messages = []
            
        logging.info(f"✅ Finished case [{idx+1}/{total_num}] - {case_id} (Correct: {'Yes' if is_correct else 'No'}, Tool Error: {'Yes' if has_tool_error else 'No'})")
    except Exception as e:
        logging.error(f"❌ Failed case [{idx+1}/{total_num}] - {case_id}: {str(e)}")
        result_text = f"Error: {str(e)}"
        pred_sql = ""
        training_messages = []
        is_correct = 0
        has_tool_error = True
        privacy_trace = {}

    privacy_eval = None
    if item.get("_masksql_annotation"):
        try:
            privacy_eval = evaluate_privacy_for_item(item, privacy_trace, agent=agent, enable_ri_attack=enable_ri_attack)
        except Exception as e:
            logging.error(f"❌ Privacy evaluation failed for case {case_id}: {str(e)}")
            privacy_eval = {"error": str(e)}

    res_data = {
        "idx": idx,
        "case_id": case_id,
        "db_id": db_id,
        "question": item.get('question', ''),
        "gold_sql": gold_sql,
        "result_text": result_text,
        "pred_sql": pred_sql,
        "training_messages": training_messages,
        "is_correct": is_correct,
        "has_tool_error": has_tool_error,
        "masksql_idx": (item.get("_masksql_annotation") or {}).get("masksql_idx"),
        "gold_schema_links": (item.get("_masksql_annotation") or {}).get("orig_gold_schema_links", {}),
        "gold_value_links": (item.get("_masksql_annotation") or {}).get("orig_gold_value_links", {}),
        "masked_question": privacy_trace.get("masked_question", "") if isinstance(privacy_trace, dict) else "",
        "masked_schema": privacy_trace.get("masked_schema", "") if isinstance(privacy_trace, dict) else "",
        "masked_sql": privacy_trace.get("masked_sql", "") if isinstance(privacy_trace, dict) else "",
        "unmask_dict": privacy_trace.get("unmask_dict", {}) if isinstance(privacy_trace, dict) else {},
        "privacy_eval": privacy_eval,
    }
    
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(res_data, f, ensure_ascii=False, indent=2)
        
    return res_data


# ==========================================
# Data Generation Process Core Logic
# ==========================================
def run_generation(agent: PrivacyProtectionAgent, data_path: str, db_root_path: str, output_dir: str, inference_data_name: str, train_data_dir: str, max_workers: int = 20):
    logging.info(f"Loading dataset: {data_path}")
    data = read_data(data_path)
    total_data = len(data)
    
    reasoning_file_dir = os.path.join(output_dir, inference_data_name, "agent_synthesis")
    cache_dir = os.path.join(reasoning_file_dir, "cache")
    os.makedirs(reasoning_file_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(train_data_dir, exist_ok=True)
    
    pred_sql_path = os.path.join(reasoning_file_dir, 'predict.sql')
    json_result_path = os.path.join(reasoning_file_dir, 'predict.json')
    all_res_path = os.path.join(reasoning_file_dir, 'all_res.json')
    training_data_path = os.path.join(train_data_dir, f"{inference_data_name}_training_data.jsonl") 

    results_list = []
    logging.info(f"⚡ Starting multi-threaded concurrent processing, max workers: {max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_item, idx, item, agent, total_data, db_root_path, cache_dir): idx 
            for idx, item in enumerate(data)
        }
        for future in as_completed(futures):
            try:
                res = future.result()
                results_list.append(res)
            except Exception as e:
                logging.error(f"Uncaught exception during thread execution: {str(e)}")

    results_list.sort(key=lambda x: x["idx"])

    all_res = []
    bird_eval_dic = {}
    
    with open(pred_sql_path, 'w', encoding='utf-8') as f_sql, \
         open(training_data_path, 'w', encoding='utf-8') as f_train:
        for res in results_list:
            f_sql.write(res["pred_sql"] + '\n')
            bird_eval_dic[res["idx"]] = res["pred_sql"] + '\t----- bird -----\t' + res["db_id"]
            
            all_res.append({
                'db_id': res["db_id"],
                'input': res["question"],
                'output': res["result_text"],
                'pred_sql': res["pred_sql"],
                'gold_sql': res["gold_sql"],
                'is_correct': res.get("is_correct", 0),
                'has_tool_error': res.get("has_tool_error", False),
                'masksql_idx': res.get("masksql_idx"),
                'masked_question': res.get("masked_question", ""),
                'masked_sql': res.get("masked_sql", ""),
                'privacy_eval': res.get("privacy_eval"),
            })

            if res.get("training_messages"):
                train_record = {"messages": res["training_messages"]}
                f_train.write(json.dumps(train_record, ensure_ascii=False) + '\n')

    with open(json_result_path, 'w', encoding='utf-8') as f_json:
        json.dump(bird_eval_dic, f_json, indent=4)
        
    with open(all_res_path, 'w', encoding='utf-8') as f_all:
        json.dump(all_res, f_all, indent=4)

    logging.info(f"✅ Data synthesis completed! Check outputs in: {reasoning_file_dir}")
    logging.info(f"✅ Correctly generated SQLs for training are saved in: {training_data_path}")


def run_privacy_evaluation(
    agent: PrivacyProtectionAgent,
    data_path: str,
    masksql_test_path: str,
    db_root_path: str,
    output_dir: str,
    inference_data_name: str = "bird_dev_sc_masksql_overlap",
    train_data_dir: Optional[str] = None,
    max_workers: int = 20,
    enable_ri_attack: bool = False,
):
    """
    Run the agent only on BIRD examples that have MaskSQL human schema/value-link annotations,
    then compute MaskSQL-style privacy metrics.
    """
    logging.info(f"Loading BIRD dataset: {data_path}")
    data = read_data(data_path)

    reasoning_file_dir = os.path.join(output_dir, inference_data_name, "privacy_eval")
    cache_dir = os.path.join(reasoning_file_dir, "cache")
    os.makedirs(reasoning_file_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)
    if train_data_dir:
        os.makedirs(train_data_dir, exist_ok=True)

    unmatched_path = os.path.join(reasoning_file_dir, "unmatched_bird_items.json")
    matched_data, match_stats = select_masksql_annotated_subset(
        bird_data=data,
        masksql_test_path=masksql_test_path,
        unmatched_save_path=unmatched_path,
    )
    total_data = len(matched_data)

    matched_data_path = os.path.join(reasoning_file_dir, "matched_masksql_annotated_data.json")
    with open(matched_data_path, "w", encoding="utf-8") as f:
        json.dump(matched_data, f, ensure_ascii=False, indent=2)

    logging.info(f"MaskSQL overlap selection stats: {json.dumps(match_stats, ensure_ascii=False)}")
    logging.info(f"⚡ Starting privacy evaluation on {total_data} matched cases, max_workers={max_workers}, RI attack={enable_ri_attack}")

    results_list: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_single_item,
                idx,
                item,
                agent,
                total_data,
                db_root_path,
                cache_dir,
                enable_ri_attack,
            ): idx
            for idx, item in enumerate(matched_data)
        }
        for future in as_completed(futures):
            try:
                results_list.append(future.result())
            except Exception as e:
                logging.error(f"Uncaught exception during privacy evaluation thread execution: {str(e)}")

    results_list.sort(key=lambda x: x["idx"])
    summary = summarize_privacy_results(results_list)
    summary.update(match_stats)
    summary["ri_attack_enabled"] = enable_ri_attack

    pred_sql_path = os.path.join(reasoning_file_dir, "predict.sql")
    json_result_path = os.path.join(reasoning_file_dir, "predict.json")
    all_res_path = os.path.join(reasoning_file_dir, "all_res.json")
    privacy_res_path = os.path.join(reasoning_file_dir, "privacy_all_res.json")
    summary_path = os.path.join(reasoning_file_dir, "privacy_summary.json")
    training_data_path = os.path.join(train_data_dir, f"{inference_data_name}_training_data.jsonl") if train_data_dir else None

    bird_eval_dic = {}
    all_res = []
    with open(pred_sql_path, "w", encoding="utf-8") as f_sql:
        f_train = open(training_data_path, "w", encoding="utf-8") if training_data_path else None
        try:
            for res in results_list:
                f_sql.write(res.get("pred_sql", "") + "\n")
                bird_eval_dic[res["idx"]] = res.get("pred_sql", "") + "\t----- bird -----\t" + res.get("db_id", "")
                all_res.append({
                    "idx": res.get("idx"),
                    "case_id": res.get("case_id"),
                    "masksql_idx": res.get("masksql_idx"),
                    "db_id": res.get("db_id"),
                    "input": res.get("question", ""),
                    "output": res.get("result_text", ""),
                    "pred_sql": res.get("pred_sql", ""),
                    "gold_sql": res.get("gold_sql", ""),
                    "is_correct": res.get("is_correct", 0),
                    "has_tool_error": res.get("has_tool_error", False),
                    "gold_schema_links": res.get("gold_schema_links", {}),
                    "gold_value_links": res.get("gold_value_links", {}),
                    "masked_question": res.get("masked_question", ""),
                    "masked_schema": res.get("masked_schema", ""),
                    "masked_sql": res.get("masked_sql", ""),
                    "unmask_dict": res.get("unmask_dict", {}),
                    "privacy_eval": res.get("privacy_eval"),
                })
                if f_train and res.get("training_messages"):
                    f_train.write(json.dumps({"messages": res["training_messages"]}, ensure_ascii=False) + "\n")
        finally:
            if f_train:
                f_train.close()

    with open(json_result_path, "w", encoding="utf-8") as f:
        json.dump(bird_eval_dic, f, ensure_ascii=False, indent=2)
    with open(all_res_path, "w", encoding="utf-8") as f:
        json.dump(all_res, f, ensure_ascii=False, indent=2)
    with open(privacy_res_path, "w", encoding="utf-8") as f:
        json.dump(results_list, f, ensure_ascii=False, indent=2)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logging.info(f"✅ Privacy evaluation completed. Results: {reasoning_file_dir}")
    logging.info(f"📊 Privacy summary: {json.dumps(summary, ensure_ascii=False, indent=2)}")
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Privacy-preserving NL2SQL agent with MaskSQL-style privacy evaluation")
    parser.add_argument("--mode", choices=["generation", "privacy_eval"], default="privacy_eval")
    parser.add_argument("--base_url", default=os.getenv("BASE_URL", "https://api.deepseek.com/v1"))
    parser.add_argument("--model_name", default=os.getenv("MODEL_NAME", "deepseek-v4-pro"))
    parser.add_argument("--api_key", default=os.getenv("OPENAI_API_KEY", "EMPTY"))
    parser.add_argument("--data_path", default=str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data/bird_dev_sc.jsonl'))
    parser.add_argument("--masksql_test_path", default=str(_RELEASE_ROOT / 'privacy-agent/MaskSQL/data/all_chunks.json'))
    parser.add_argument("--db_root_path", default=str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1'))
    parser.add_argument("--output_dir", default=str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/output'))
    parser.add_argument("--train_data_dir", default=str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/train_data'))
    parser.add_argument("--inference_data_name", default="bird_dev_sc_masksql_overlap")
    parser.add_argument("--max_workers", type=int, default=20)
    parser.add_argument("--enable_ri_attack", action="store_true", help="Enable LLM adversary attack for RI score; disabled by default to save cost.")
    args = parser.parse_args()

    agent = PrivacyProtectionAgent(base_url=args.base_url, model_name=args.model_name, api_key=args.api_key)

    if args.mode == "privacy_eval":
        run_privacy_evaluation(
            agent=agent,
            data_path=args.data_path,
            masksql_test_path=args.masksql_test_path,
            db_root_path=args.db_root_path,
            output_dir=args.output_dir,
            inference_data_name=args.inference_data_name,
            train_data_dir=args.train_data_dir,
            max_workers=args.max_workers,
            enable_ri_attack=args.enable_ri_attack,
        )
    else:
        run_generation(
            agent=agent,
            data_path=args.data_path,
            db_root_path=args.db_root_path,
            output_dir=args.output_dir,
            inference_data_name=args.inference_data_name,
            train_data_dir=args.train_data_dir,
            max_workers=args.max_workers,
        )
