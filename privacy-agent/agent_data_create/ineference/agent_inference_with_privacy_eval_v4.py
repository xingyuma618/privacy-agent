
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))

import os
import json
import logging
from openai import OpenAI
import httpx
from typing import Dict, List, Any, Tuple
import re
import copy
import subprocess
import time
import traceback
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed 
from statistics import mean
from tools import generate_schema_string_from_masked, execute_sql,  find_result, safe_execute_sql
from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template

# Uncomment if you need specific dependencies
# from transformers import AutoModelForCausalLM, AutoTokenizer
# import sqlglot
# import sqlite3
import json_repair  

# Import your existing template files
from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template
from tools import generate_schema_string_from_masked

# Configure logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==========================================
# Utility functions: SQL cleaning and extraction
# ==========================================
def clean_space(sql):
    sql = re.sub(r'\s*\)', ')', sql)
    # Aggregate function brackets need to be connected
    sql = re.sub(r'min\s*\(\s*', 'min(', sql)
    sql = re.sub(r'max\s*\(\s*', 'max(', sql)
    sql = re.sub(r'sum\s*\(\s*', 'sum(', sql)
    sql = re.sub(r'avg\s*\(\s*', 'avg(', sql)
    sql = re.sub(r'count\s*\(\s*', 'count(', sql)
    sql = re.sub(r'MIN\s*\(\s*', 'MIN(', sql)
    sql = re.sub(r'MAX\s*\(\s*', 'MAX(', sql)
    sql = re.sub(r'SUM\s*\(\s*', 'SUM(', sql)
    sql = re.sub(r'AVG\s*\(\s*', 'AVG(', sql)
    sql = re.sub(r'COUNT\s*\(\s*', 'COUNT(', sql)
    # For non-aggregate functions, only delete spaces to the right of the left bracket
    sql = re.sub(r'\(\s*', '(', sql)
    # Delete extra spaces
    sql = re.sub(r' +', ' ', sql)
    sql = re.sub(r'\s*,', ',', sql) 
    return sql

def find_result(text, language='sql'):
    sql = text
    if '```sql' in text:
        pattern = r"```sql(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        if len(matches) > 0:
            sql = matches[-1].replace('\n', ' ').strip()
            sql = clean_space(sql)
            return sql
        
    sql = sql.replace('\n', ' ').strip()
    return sql

def read_data(path, encoding="utf-8"):
    # 1. Read data as a list based on different formats
    if path.endswith('.jsonl'):
        with open(path, "r", encoding=encoding) as f:
            data = [json.loads(line) for line in f if line.strip()]
    else:
        with open(path, "r", encoding=encoding) as f:
            data = json.load(f)
            
    # 2. Sort in ascending order based on question_id
    sorted_data = sorted(data, key=lambda x: int(x.get('question_id', 0)))
    

    return sorted_data


def _coerce_json_object(value: Any, default: Any = None, max_depth: int = 3) -> Any:
    """
    Robustly parse model-emitted tool-call payloads.

    Some local OpenAI-compatible servers return function arguments as a JSON
    string, and some models emit nested JSON strings such as:
      {"name": "query_rewrite", "arguments": "{\"query_to_rewrite\": \"...\"}"}
    The normal tool execution code expects a dict, so we recursively parse
    strings until we either get a structured object or give up.
    """
    if default is None:
        default = {}
    current = value
    for _ in range(max_depth):
        if isinstance(current, (dict, list)):
            return current
        if current is None:
            return default
        if isinstance(current, str):
            text = current.strip()
            if not text:
                return default
            try:
                current = json_repair.loads(text)
                continue
            except Exception:
                try:
                    current = json.loads(text)
                    continue
                except Exception:
                    return default
        return default
    return current if isinstance(current, (dict, list)) else default


def _coerce_tool_arguments(arguments: Any) -> Dict[str, Any]:
    """Return a dict for tool arguments even when the model emitted malformed args."""
    parsed = _coerce_json_object(arguments, default={})
    if isinstance(parsed, dict):
        return parsed
    # Do not crash if the model mistakenly emits a list or scalar as arguments.
    return {"_raw_arguments": parsed}


def _normalize_tool_call_payload(tool_data: Any) -> Tuple[str, Dict[str, Any]]:
    """Normalize multiple possible tool-call JSON shapes into (name, args)."""
    tool_obj = _coerce_json_object(tool_data, default={})
    if not isinstance(tool_obj, dict):
        return "", {}

    # Common custom shape: {"name": ..., "arguments": {...}}
    name = tool_obj.get("name") or tool_obj.get("tool") or tool_obj.get("function_name") or ""
    arguments = tool_obj.get("arguments", {})

    # OpenAI native-like shape: {"function": {"name": ..., "arguments": ...}}
    function_obj = tool_obj.get("function")
    if isinstance(function_obj, dict):
        name = function_obj.get("name", name)
        arguments = function_obj.get("arguments", arguments)

    return str(name).strip(), _coerce_tool_arguments(arguments)

# ==========================================
# MaskSQL privacy annotation matching and metrics
# ==========================================
def _norm_text_for_match(value: Any) -> str:
    """Normalize text fields for robust dataset matching."""
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").strip()
    return re.sub(r"\s+", " ", text)


def _norm_sql_for_match(sql: Any) -> str:
    """Normalize SQL enough for identifying the same BIRD example."""
    text = _norm_text_for_match(sql).rstrip(";")
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\s*,\s*", ",", text)
    text = re.sub(r"\s+", " ", text)
    return text.lower()


def _bird_match_key(item: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        _norm_text_for_match(item.get("db_id")),
        _norm_text_for_match(item.get("question")),
        _norm_text_for_match(item.get("evidence")),
        _norm_sql_for_match(item.get("gold_sql")),
    )


def _masksql_match_key(item: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        _norm_text_for_match(item.get("orig_db_id")),
        _norm_text_for_match(item.get("orig_question")),
        _norm_text_for_match(item.get("orig_evidence")),
        _norm_sql_for_match(item.get("orig_gold_sql")),
    )


def _safe_question_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _masksql_question_id(item: Dict[str, Any]) -> str:
    idx = str(item.get("idx", ""))
    if idx.startswith("bird_"):
        return idx.split("_", 1)[1]
    return idx


def _combine_gold_links(masksql_row: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Return one annotation dictionary keyed by sensitive NL surface term."""
    combined: Dict[str, Dict[str, str]] = {}
    for term, target in (masksql_row.get("orig_gold_schema_links") or {}).items():
        combined[str(term)] = {"type": "schema", "target": str(target)}
    for term, target in (masksql_row.get("orig_gold_value_links") or {}).items():
        if str(term) in combined:
            combined[str(term)] = {
                "type": combined[str(term)]["type"] + "+value",
                "target": combined[str(term)]["target"] + " || " + str(target),
            }
        else:
            combined[str(term)] = {"type": "value", "target": str(target)}
    return combined


def load_masksql_test(masksql_test_path: str) -> List[Dict[str, Any]]:
    with open(masksql_test_path, "r", encoding="utf-8") as f:
        return json.load(f)


def filter_and_attach_masksql_annotations(
    data: List[Dict[str, Any]],
    masksql_test_path: str,
    match_mode: str = "auto",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Select BIRD rows that have MaskSQL human annotations and attach annotation fields.

    match_mode:
      - strict: use (db_id, question, evidence, gold_sql) only.
      - relaxed: strict first, then ignore evidence if the other fields match.
      - id: strict first, then use MaskSQL idx=bird_{question_id} fallback.
      - auto: strict -> relaxed -> id. This is useful when local BIRD files have
              evidence or SQL formatting drift from MaskSQL's released annotations.
    """
    mask_rows = load_masksql_test(masksql_test_path)
    strict_index = {_masksql_match_key(row): row for row in mask_rows}
    relaxed_index = {
        (_masksql_match_key(row)[0], _masksql_match_key(row)[1], _masksql_match_key(row)[3]): row
        for row in mask_rows
    }
    id_index = {(_masksql_question_id(row), _norm_text_for_match(row.get("orig_db_id"))): row for row in mask_rows}

    selected: List[Dict[str, Any]] = []
    used_masksql_ids = set()
    match_counter = {"strict": 0, "relaxed": 0, "id": 0, "unmatched": 0}

    for item in data:
        strict_key = _bird_match_key(item)
        relaxed_key = (strict_key[0], strict_key[1], strict_key[3])
        qid_key = (_safe_question_id(item.get("question_id")), _norm_text_for_match(item.get("db_id")))
        match_row = None
        method = None

        if strict_key in strict_index:
            match_row = strict_index[strict_key]
            method = "strict"
        elif match_mode in {"relaxed", "auto"} and relaxed_key in relaxed_index:
            match_row = relaxed_index[relaxed_key]
            method = "relaxed"
        elif match_mode in {"id", "auto"} and qid_key in id_index:
            match_row = id_index[qid_key]
            method = "id"

        if match_row is None:
            match_counter["unmatched"] += 1
            continue

        match_counter[method] += 1
        used_masksql_ids.add(match_row.get("idx"))
        new_item = copy.deepcopy(item)
        new_item["masksql_idx"] = match_row.get("idx")
        new_item["masksql_match_method"] = method
        new_item["masksql_orig_question"] = match_row.get("orig_question", "")
        new_item["masksql_orig_evidence"] = match_row.get("orig_evidence", "")
        new_item["masksql_orig_gold_sql"] = match_row.get("orig_gold_sql", "")
        new_item["orig_gold_schema_links"] = match_row.get("orig_gold_schema_links", {}) or {}
        new_item["orig_gold_value_links"] = match_row.get("orig_gold_value_links", {}) or {}
        new_item["annotated_links"] = _combine_gold_links(match_row)
        selected.append(new_item)

    report = {
        "masksql_test_path": masksql_test_path,
        "match_mode": match_mode,
        "input_rows": len(data),
        "masksql_rows": len(mask_rows),
        "selected_rows": len(selected),
        "matched_masksql_rows": len(used_masksql_ids),
        "match_counter": match_counter,
    }
    return selected, report


def _norm_surface(value: Any) -> str:
    text = _norm_text_for_match(value).strip("'\"` ").lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _surface_in_text(term: str, text: str) -> bool:
    term_norm = _norm_surface(term)
    text_norm = _norm_surface(text)
    if not term_norm:
        return False
    if re.fullmatch(r"[a-z0-9_]+", term_norm):
        return re.search(r"(?<![a-z0-9_])" + re.escape(term_norm) + r"(?![a-z0-9_])", text_norm) is not None
    return term_norm in text_norm


def _term_covered_by_masked_terms(term: str, masked_terms: List[str]) -> bool:
    term_norm = _norm_surface(term)
    if not term_norm:
        return False
    for masked_term in masked_terms or []:
        mt = _norm_surface(masked_term)
        if not mt:
            continue
        if term_norm == mt or term_norm in mt or mt in term_norm:
            return True
    return False


ATTACK_PROMPT_AGENT_V1 = """
Your goal is to guess the original words in a masked Text-to-SQL input. Given
(1) NL Text: a masked natural-language instruction/question for extracting information from a database
(2) DB Schema: the masked database schema used by the remote SQL-generation model

guess the original word or phrase for each masked symbol that appears in the NL text.

Important: This prompt is adapted from MaskSQL's attack_v2 prompt to match this agent's mask format.

Input Format:
- DB Schema is the masked schema text produced by the privacy agent. It may be semi-structured rather than YAML.
- Table names, column names, and literal values are replaced consistently in both NL Text and DB Schema.
- Some column descriptions or value descriptions may also be masked.

Mask Format:
- table_1, table_2, ... are symbols used to mask table names.
- col_1, col_2, ... are symbols used to mask column names.
- val_1, val_2, ... are symbols used to mask literal values.
- The same symbol always refers to the same original object/value inside this case.

Output Rules:
- Output ONLY the unmasked NL text.
- The output should contain all of the unmasked tokens in the NL text; no portion of the text should be omitted.
- Do not output a JSON object, dictionary, bullet list, explanation, or comments.
- If you cannot confidently recover a mask, make your best guess while keeping the original sentence structure.
- For column names with spaces, wrap them in backticks, e.g. `car model`.

Example 1:
NL Text: What is the col_1 of table_1 who have a table_2 with col_4 higher than val_1? val_1 is a value of col_4
DB Schema:
table_1:
  col_1: text
  col_2: text
table_2:
  col_3: text
  col_4: number

OUTPUT:
What is the name of people who have a car with horse power higher than 1000? 1000 is a value of horse power

Now, unmask the following text considering the following DB schema.
Inputs:
NL Text: {question}
DB Schema:
{schema}
"""


def _masksql_exact_attack_recovered(term: str, attack_text: str) -> bool:
    """Match MaskSQL Results._process_row: term.lower() in attack.lower()."""
    if term is None or attack_text is None:
        return False
    term_text = str(term).lower()
    attack = str(attack_text).lower()
    return term_text in attack


def compute_privacy_metrics_for_result(res: Dict[str, Any], attack_text: str = "") -> Dict[str, Any]:
    """
    MaskSQL-style privacy evaluation.

    MR follows MaskSQL's released code idea: annotated sensitive terms are counted
    as covered when they appear in the list of original terms actually masked by
    the system. RI follows MaskSQL's exact-match attack scoring when attack_text
    is supplied; otherwise it is left as None.
    """
    annotated_links = res.get("annotated_links") or {}
    annotated_terms = list(annotated_links.keys())
    masked_terms = res.get("masked_terms") or []
    masked_question = res.get("masked_question") or ""
    masked_schema = res.get("masked_schema") or ""
    masked_prompt = f"{masked_question}\n{masked_schema}"
    original_prompt = f"{res.get('question', '')} {res.get('evidence', '')} {res.get('final_question', '')}"

    covered_terms = [t for t in annotated_terms if _term_covered_by_masked_terms(t, masked_terms)]
    terms_present_in_input = [t for t in annotated_terms if _surface_in_text(t, original_prompt)]
    covered_present_terms = [t for t in terms_present_in_input if _term_covered_by_masked_terms(t, masked_terms)]
    direct_leak_terms = [t for t in annotated_terms if _surface_in_text(t, masked_prompt)]

    denominator = len(annotated_terms)
    present_denominator = len(terms_present_in_input)
    mr = (len(covered_terms) / denominator) if denominator else 1.0
    mr_present = (len(covered_present_terms) / present_denominator) if present_denominator else 1.0
    prompt_privacy_score = (1.0 - len(direct_leak_terms) / denominator) if denominator else 1.0

    ri = None
    recovered_terms = []
    if attack_text:
        # Follow MaskSQL's public implementation: a mask is considered
        # re-identified when the original masked term is a lowercase substring
        # of the attacker's generated unmasked NL text.
        recovered_terms = [t for t in masked_terms if _masksql_exact_attack_recovered(t, attack_text)]
        ri = 1.0 - (len(recovered_terms) / len(masked_terms)) if masked_terms else 1.0

    return {
        "masksql_idx": res.get("masksql_idx"),
        "case_id": res.get("case_id"),
        "db_id": res.get("db_id"),
        "match_method": res.get("masksql_match_method"),
        "gt_sensitive_count": denominator,
        "gt_sensitive_present_count": present_denominator,
        "masked_terms_count": len(masked_terms),
        "masked_cover_count": len(covered_terms),
        "masked_cover_present_count": len(covered_present_terms),
        "MR": mr,
        "MR_present": mr_present,
        "prompt_direct_leak_count": len(direct_leak_terms),
        "prompt_direct_leak_terms": direct_leak_terms,
        "prompt_privacy_score": prompt_privacy_score,
        "RI": ri,
        "attack_recovered_count": len(recovered_terms),
        "attack_recovered_terms": recovered_terms,
        "annotated_terms": annotated_terms,
        "covered_terms": covered_terms,
    }


def _run_reidentification_attack(client: OpenAI, model_name: str, masked_question: str, masked_schema: str) -> str:
    """Optional RI attack following MaskSQL attack_v2 semantics.

    MaskSQL asks the attacker to output the unmasked NL text, not a token list.
    This version keeps that assumption but adapts the prompt from [T1]/[C1]/[V1]
    masks to this agent's table_1/col_1/val_1 masks and schema text format.
    """
    if not str(masked_question or "").strip():
        return ""
    prompt = ATTACK_PROMPT_AGENT_V1.format(
        question=str(masked_question or ""),
        schema=str(masked_schema or ""),
    )
    messages = [{"role": "user", "content": prompt}]
    response = client.chat.completions.create(model=model_name, messages=messages, temperature=0)
    return response.choices[0].message.content.strip()


def evaluate_privacy_results(
    results_list: List[Dict[str, Any]],
    output_path: str,
    attack_client: OpenAI = None,
    attack_model_name: str = "",
) -> Dict[str, Any]:
    per_case = []
    for res in results_list:
        attack_text = ""
        # MaskSQL only scores RI over the actual masked_terms. If there are no
        # masked terms, skip the remote attack call and let RI remain None/1.0
        # according to compute_privacy_metrics_for_result.
        if attack_client is not None and attack_model_name and res.get("masked_terms"):
            try:
                attack_text = _run_reidentification_attack(
                    attack_client,
                    attack_model_name,
                    res.get("masked_question", ""),
                    res.get("masked_schema", ""),
                )
            except Exception as e:
                logging.warning(f"RI attack failed for case {res.get('case_id')}: {e}")
                attack_text = ""
        metrics = compute_privacy_metrics_for_result(res, attack_text=attack_text)
        if attack_text:
            metrics["attack_text"] = attack_text
        per_case.append(metrics)

    def _avg(key: str):
        vals = [row[key] for row in per_case if row.get(key) is not None]
        return mean(vals) if vals else None

    aggregate = {
        "num_cases": len(per_case),
        "MR": _avg("MR"),
        "MR_present": _avg("MR_present"),
        "prompt_privacy_score": _avg("prompt_privacy_score"),
        "RI": _avg("RI"),
        "total_gt_sensitive_count": sum(row["gt_sensitive_count"] for row in per_case),
        "total_masked_cover_count": sum(row["masked_cover_count"] for row in per_case),
        "total_prompt_direct_leak_count": sum(row["prompt_direct_leak_count"] for row in per_case),
    }
    report = {"aggregate": aggregate, "per_case": per_case}
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return report
        
class PrivacyProtectionAgent:
    def __init__(self, base_url: str, model_name: str, api_key: str = "EMPTY"):
        self.client = OpenAI(api_key=api_key, base_url=base_url, http_client=httpx.Client(trust_env=False))
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
        rewrite_client = OpenAI(
            api_key=os.environ.get("QUERY_REWRITE_API_KEY", "EMPTY"),
            base_url=os.environ.get("QUERY_REWRITE_BASE_URL", "http://8.130.67.139:8037/v1"),
            http_client=httpx.Client(trust_env=False),
        )
        model_name = os.environ.get(
            "QUERY_REWRITE_MODEL_NAME",
            str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-50'),
        )
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
        entity_extract_client = OpenAI(
            api_key=os.environ.get("ENTITY_EXTRACT_API_KEY", "EMPTY"),
            base_url=os.environ.get("ENTITY_EXTRACT_BASE_URL", "http://8.130.67.139:8036/v1"),
            http_client=httpx.Client(trust_env=False),
        )
        model_name = os.environ.get(
            "ENTITY_EXTRACT_MODEL_NAME",
            str(_RELEASE_ROOT / 'LlamaFactory/saves/entity_extract_v1/checkpoint-25'),
        )
        response = entity_extract_client.chat.completions.create(model=model_name, messages=messages, temperature=self.temperature)
        response_text = response.choices[0].message.content.strip()
        match = re.search(r'\[\s*\{.*?\}\s*\]', response_text, re.DOTALL)
        return match.group(0) if match else "[]"
    
    def _tool_sql_generate(self, masked_query, masked_schema_str, logger=None):
        sql_generate_client = OpenAI(
            api_key=os.environ.get("SQL_GENERATE_API_KEY", "EMPTY"),
            base_url=os.environ.get("SQL_GENERATE_BASE_URL", "https://api.deepseek.com/v1"),
            http_client=httpx.Client(trust_env=False),
        )
        model_name = os.environ.get("SQL_GENERATE_MODEL_NAME", 'deepseek-chat')

        # sql_generate_client = OpenAI(
        #     api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),  
        #     base_url="http://127.0.0.1:2333/v1"  
        # )
        # model_name = '/root/models/Qwen2.5-72B-Instruct'

        input_str = f"- Database Schema:\n{masked_schema_str}\n- User's Question:\n{masked_query}"
        messages = get_sql_generate_template(input_str)
        response = sql_generate_client.chat.completions.create(model=model_name, messages=messages, temperature=self.temperature)
        response_text = response.choices[0].message.content.strip()
        
        return response_text

    def _tool_sql_unmask(self, masked_sql: str, unmask_dict: Any) -> str:
        if isinstance(unmask_dict, str):
            unmask_dict = json_repair.loads(unmask_dict)
        if not masked_sql or not isinstance(masked_sql, str) or not isinstance(unmask_dict, dict):
            return masked_sql

        def quote_identifier(identifier: str) -> str:
            return "`" + identifier.replace("`", "``") + "`"

        def repair_qualified_identifier_quotes(sql: str, column_names: set[str]) -> str:
            if not sql:
                return sql

            def repl(match):
                table_col = match.group(1)
                if table_col not in column_names or "." not in table_col:
                    return match.group(0)
                table, col = table_col.split(".", 1)
                return f"{quote_identifier(table)}.{quote_identifier(col)}"

            # Some SQL generators quote a fully-qualified column as one token,
            # e.g. "singer.Name". SQLite treats that as a string literal/invalid
            # identifier rather than singer.Name, causing executable but wrong SQL.
            sql = re.sub(r'"([A-Za-z_][\w]*\.[A-Za-z_][\w]*)"', repl, sql)
            sql = re.sub(r'`([A-Za-z_][\w]*\.[A-Za-z_][\w]*)`', repl, sql)
            sql = re.sub(r'\[([A-Za-z_][\w]*\.[A-Za-z_][\w]*)\]', repl, sql)
            return sql

        mask_info_list = []
        qualified_columns = set()
        for category in ["tables", "columns", "values"]:
            if category in unmask_dict:
                for mask, orig in unmask_dict[category].items():
                    orig_text = str(orig)
                    mask_info_list.append({"mask": mask, "original": orig_text, "category": category})
                    if category == "columns" and "." in orig_text:
                        qualified_columns.add(orig_text)
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
        unmasked_text = repair_qualified_identifier_quotes(unmasked_text, qualified_columns)
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
        masked_terms = []
        mask_mapping = {}
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
                before = masked_query
                masked_query = pattern.sub(mask, masked_query)
                if before != masked_query:
                    masked_terms.append(ent_str)
                    mask_mapping[ent_str] = mask

        # Fallback replacement can also mask schema/table/value terms not explicitly
        # listed as extracted entities. Record those replacements for MR evaluation.
        before_fallback = masked_query
        masked_query = replace_in_string(masked_query)
        fallback_replace_dict = {orig: mask for orig, mask in table_map.items()}
        fallback_replace_dict.update({orig_c: mask for (orig_t, orig_c), mask in col_map.items()})
        fallback_replace_dict.update({orig: mask for orig, mask in val_map.items()})
        for orig, mask in fallback_replace_dict.items():
            if _surface_in_text(mask, masked_query) and _surface_in_text(orig, before_fallback):
                if str(orig) not in mask_mapping:
                    masked_terms.append(str(orig))
                    mask_mapping[str(orig)] = mask

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
        dedup_masked_terms = list(dict.fromkeys(masked_terms))
        return {
            "masked_query": masked_query,
            "masked_schema_str": masked_schema_str,
            "unmask_dict": unmask_dict,
            "masked_terms": dedup_masked_terms,
            "mask_mapping": mask_mapping,
        }

    def process(self, item: Dict[str, Any], case_id: str = "default") -> Dict[str, str]:
        
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f"case_{case_id}.txt")
        
        if os.path.exists(log_file_path):
            open(log_file_path, 'w', encoding='utf-8').close()

        def log_to_file(text: str):
            with open(log_file_path, "a", encoding="utf-8") as f:
                f.write(text + "\n")

        original_question = item['question']
        db_schema = item.get('db_schema', '') 
        evidence = item.get("evidence", '')
        db_path = db_base_dir + '/' + item['db_id'] +'/' + item['db_id'] + '.sqlite'
        structured_db_schema = item.get('structured_db_schema', {}) 
        
        user_prompt = f"- Database Schema:\n{db_schema}\n\n- User Question:\n{original_question} {evidence}\n\nTask started. Please provide your analysis (Thought) and call the tools (Action)."

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        masked_schema = ''
        masked_sql = ''
        unmask_dict = {}
        masked_question = ''
        masked_terms = []
        mask_mapping = {}
        final_question = original_question  # 默认最终问题为原始问题
        
        for iteration in range(20):
            log_to_file(f"\n{'='*20} [Round {iteration + 1}] {'='*20}")
            
            # ==========================================================
            # 1. API 请求环节：强制关闭原生解析，绕过服务端 400 报错
            # ==========================================================
            max_try = 5
            response = None
            while max_try > 0:
                try:
                    response = self.client.chat.completions.create(
                        model=self.model_name, 
                        messages=messages, 
                        tools=self.tools, 
                        temperature=self.temperature
                    )
                    break
                except Exception as e:
                    logging.error(f"API 调用失败 (剩余重试 {max_try-1}): {str(e)}")
                    max_try -= 1
                    # time.sleep(2)
                    continue

            if not response:
                log_to_file("API 调用多次重试后彻底失败")
                break
                
            message = response.choices[0].message
            original_content = message.content or ""
            
            # ==========================================================
            # 2. 客户端解析环节：只提取第一个工具，强行切断复读
            # ==========================================================
            parsed_tool_calls = []
            
            # 使用 re.search 只匹配文本中出现的第一个 <tool_call>
            match = re.search(r'<tool_call>\s*(.*?)\s*</tool_call>', original_content, re.DOTALL)
            
            if match:
                try:
                    tool_name, tool_args = _normalize_tool_call_payload(match.group(1))
                    if not tool_name:
                        raise ValueError("tool_call payload has no tool name")
                    parsed_tool_calls.append({
                        "name": tool_name,
                        "arguments": tool_args,
                        "id": f"call_patch_{iteration}_0" # 只有一个工具，固定编号 0
                    })
                except Exception as e:
                    log_to_file(f"⚠️ 工具解析失败: {e}\n原文: {match.group(1)}")

            # 打印 Thought 日志（把后面的所有 XML 标签都清洗掉）
            thought_text = re.sub(r'<tool_call>.*?</tool_call>', '', original_content, flags=re.DOTALL).strip()
            log_to_file(f"🧠 [Thought]:\n{thought_text if thought_text else '(Direct tool output)'}\n")

            # ==========================================================
            # 3. 构造合法的 Assistant 历史消息，骗过服务端的历史校验
            # ==========================================================
            assistant_msg = {"role": "assistant", "content": original_content}
            
            if parsed_tool_calls:
                # 🌟 核心机制：如果我们自己解析到了工具，我们需要手动伪造一个原生的 tool_calls 列表
                # 否则下一轮发给 API 时，服务端会报错 "tool_calls cannot be empty"
                assistant_msg["tool_calls"] = [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": json.dumps(tc["arguments"], ensure_ascii=False) # 变回干净的无换行字符串
                        }
                    }
                    for tc in parsed_tool_calls
                ]
            
            # 追加合法的助手消息
            messages.append(assistant_msg) 

            # ==========================================================
            # 4. 执行工具逻辑，并追加 Tool 历史消息
            # ==========================================================
            if parsed_tool_calls:
                for tc_data in parsed_tool_calls:
                    func_name = tc_data["name"]
                    args = _coerce_tool_arguments(tc_data.get("arguments", {}))
                    tool_call_id = tc_data["id"]
                    log_args_str = json.dumps(args, ensure_ascii=False, indent=2)
                    
                    log_to_file(f"🛠️ [Action - Tool Call]: {func_name}")
                    log_to_file(f"   [Action - Arguments]: {log_args_str}\n")
                    
                    # --- 执行你的工具逻辑 ---
                    try:
                        if func_name == "query_rewrite": 
                            result = self._tool_query_rewrite(db_schema, args.get('query_to_rewrite', ''), args.get('rewrite_instruction', ''), logger=log_to_file)
                            if isinstance(result, str) and result.strip():
                                final_question = result.strip()
                        elif func_name == "entity_extract": 
                            result = self._tool_entity_extract(db_schema, args.get('query_to_extract', ''), logger=log_to_file)
                        elif func_name == "apply_typed_masking": 
                            entities_arg = args.get('entities', [])
                            if isinstance(entities_arg, str):
                                entities_arg = _coerce_json_object(entities_arg, default=[])
                            if not isinstance(entities_arg, list):
                                entities_arg = []
                            result = self._tool_apply_typed_masking(args.get('query', ''), entities_arg, structured_db_schema)
                            if isinstance(result, dict):
                                masked_schema = result.get("masked_schema_str", masked_schema)
                                masked_question = result.get("masked_query", masked_question)
                                unmask_dict = result.get("unmask_dict", unmask_dict)
                                masked_terms = result.get("masked_terms", masked_terms)
                                mask_mapping = result.get("mask_mapping", mask_mapping)
                        elif func_name == "sql_generate":
                            masked_schema = args.get('masked_schema_str', '')
                            masked_question = args.get('masked_query', '')
                            result = self._tool_sql_generate(args.get('masked_query', ''), args.get('masked_schema_str', ''), logger=log_to_file)
                        elif func_name == "sql_unmask":
                            masked_sql = args.get('masked_sql', '')
                            unmask_dict = args.get('unmask_dict', {})
                            result = self._tool_sql_unmask(args.get('masked_sql', ''), unmask_dict)
                        elif func_name == "execute_sql_check":
                            result = self._tool_execute_sql_check(args.get('unmasked_sql', ''), db_path)
                        else: 
                            result = f"Error: Unknown tool {func_name}"
                    except Exception as tool_error:
                        result = f"Tool Execution Error in {func_name}: {type(tool_error).__name__}: {tool_error}"
                        log_to_file(traceback.format_exc())
                    
                    # 格式化结果
                    tool_content = json.dumps(result, ensure_ascii=False) if isinstance(result, (dict, list)) else str(result)
                    log_to_file(f"👀 [Observation - Result]:\n{tool_content}\n")
                    
                    # 追加原生的 tool 角色消息
                    messages.append({"role": "tool", "tool_call_id": tool_call_id, "name": func_name, "content": tool_content})

            else:
                # 如果代码走到这里，说明没有匹配到 <tool_call>，代表任务结束或直接作答
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
                log_to_file(f"[MASKED TERMS]:\n{json.dumps(masked_terms, ensure_ascii=False)}\n")

                log_to_file(f"✅ [Final Answer]:\n{thought_text}\n")
                
                return {
                    "result_text": thought_text,
                    "final_question": final_question,
                    "masked_question": masked_question,
                    "masked_schema": masked_schema,
                    "masked_sql": masked_sql,
                    "unmask_dict": unmask_dict,
                    "masked_terms": masked_terms,
                    "mask_mapping": mask_mapping,
                }
        log_to_file(f"\n{'='*30} [CASE TIMEOUT] {'='*30}")
        log_to_file("Agent exceeded maximum iterations.")
        return {
            "result_text": '{"error": "Agent timeout"}',
            "final_question": final_question,
            "masked_question": masked_question,
            "masked_schema": masked_schema,
            "masked_sql": masked_sql,
            "unmask_dict": unmask_dict,
            "masked_terms": masked_terms,
            "mask_mapping": mask_mapping,
        }


# ==========================================
# New Module: Independently extracted single-thread processing function
# ==========================================
def process_single_item(idx, item, agent: PrivacyProtectionAgent, total_num):
    case_id = str(item.get("question_id", f"idx_{idx}"))
    db_id = item.get('db_id', 'unknown_db')
    gold_sql = item.get('gold_sql', '')

    # BIRD execution evaluator must use the original BIRD dev index, not the
    # subset-local enumerate index. MaskSQL rows store it as "bird_{idx}".
    eval_original_idx = item.get("question_id", None)
    if eval_original_idx is None and item.get("masksql_idx"):
        masksql_idx_str = str(item.get("masksql_idx"))
        if masksql_idx_str.startswith("bird_"):
            eval_original_idx = masksql_idx_str.replace("bird_", "", 1)
    if eval_original_idx is None:
        eval_original_idx = idx
    eval_original_idx = str(eval_original_idx)
    
    logging.info(f"⏳ Starting to process case [{idx+1}/{total_num}] - {case_id}, eval_original_idx={eval_original_idx}")
    try:
        process_res = agent.process(item, case_id=case_id)
        result_text = process_res.get("result_text", "")
        final_question = process_res.get("final_question", item.get("question", ""))
        masked_question = process_res.get("masked_question", "")
        pred_sql = find_result(result_text)
        logging.info(f"✅ Finished case [{idx+1}/{total_num}] - {case_id}")
    except Exception as e:
        logging.error(f"❌ Case failed [{idx+1}/{total_num}] - {case_id}: {type(e).__name__}: {e}")
        logging.error(traceback.format_exc())
        process_res = {
            "result_text": json.dumps({
                "error": "case failed",
                "exception_type": type(e).__name__,
                "message": str(e),
            }, ensure_ascii=False),
            "final_question": item.get("question", ""),
            "masked_question": "",
            "masked_schema": "",
            "masked_sql": "",
            "unmask_dict": {},
            "masked_terms": [],
            "mask_mapping": {},
        }
        result_text = process_res["result_text"]
        final_question = process_res["final_question"]
        masked_question = process_res["masked_question"]
        pred_sql = ""

    return {
        "idx": idx,
        "eval_original_idx": eval_original_idx,
        "case_id": case_id,
        "db_id": db_id,
        "question": item.get('question', ''),
        "evidence": item.get('evidence', ''),
        "final_question": final_question,
        "masked_question": masked_question,
        "masked_schema": process_res.get("masked_schema", ""),
        "masked_sql": process_res.get("masked_sql", ""),
        "unmask_dict": process_res.get("unmask_dict", {}),
        "masked_terms": process_res.get("masked_terms", []),
        "mask_mapping": process_res.get("mask_mapping", {}),
        "gold_sql": gold_sql,
        "result_text": result_text,
        "pred_sql": pred_sql,
        "masksql_idx": item.get("masksql_idx"),
        "masksql_match_method": item.get("masksql_match_method"),
        "orig_gold_schema_links": item.get("orig_gold_schema_links", {}),
        "orig_gold_value_links": item.get("orig_gold_value_links", {}),
        "annotated_links": item.get("annotated_links", {}),
    }

# ==========================================
# Core logic of the evaluation module
# ==========================================
def eval_exec(predicted_sql_path, db_root_path, data_mode, ground_truth_path, num_cpus, mode_gt, mode_predict, diff_json_path, meta_time_out, output_dir, inference_data_name):
    if 'spider' in inference_data_name:
        predicted_sql_path = predicted_sql_path.replace('json', 'sql')
        # Spider path replacement logic. Keep environment overrides so Spider
        # privacy-generalization experiments can use the CHESS Spider copies.
        db_root_path = os.environ.get("SPIDER_DB_ROOT_PATH", db_root_path)
        table_path = os.environ.get("SPIDER_TABLE_PATH", "")
        env_gold_path = os.environ.get("SPIDER_GOLD_PATH", "")
        if 'syn' in inference_data_name:
            ground_truth_path = env_gold_path or '/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_syn_gold.sql'
            table_path = table_path or '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        elif 'real' in inference_data_name:
            ground_truth_path = env_gold_path or '/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_real_gold.sql'
            table_path = table_path or '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        elif 'test' in inference_data_name:
            ground_truth_path = env_gold_path or '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_gold.sql'
            table_path = table_path or '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_tables.json'
        elif 'dev' in inference_data_name:
            ground_truth_path = env_gold_path or '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql'
            table_path = table_path or '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        else:
            ground_truth_path = env_gold_path or ground_truth_path
            table_path = table_path or '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        
        eval_cmd = f'''nohup python spider_eval/evaluation.py \
        --gold {ground_truth_path} \
        --pred {predicted_sql_path} \
        --etype all \
        --db {db_root_path} \
        --table {table_path} > {output_dir}/eval.log 2>&1 &
        ''' 
    else:
        # BIRD logic
        eval_cmd = f'''python bird_eval/evaluation.py \
    --db_root_path {db_root_path} \
    --predicted_sql_path {predicted_sql_path} \
    --data_mode {data_mode} \
    --ground_truth_path {ground_truth_path} \
    --num_cpus {num_cpus} \
    --mode_gt {mode_gt} \
    --mode_predict {mode_predict} \
    --diff_json_path {diff_json_path} \
    --meta_time_out {meta_time_out} \
    --output_dir {output_dir}
    ''' 
    logging.info(f"🚀 Starting to execute the evaluation script...\n{eval_cmd}")
    subprocess.run(eval_cmd, shell=True, check=True)

def run_evaluation(
    agent: PrivacyProtectionAgent,
    data_path: str,
    db_root_path: str,
    output_dir: str,
    inference_data_name: str,
    diff_json_path: str,
    data_mode: str,
    ground_truth_path: str,
    max_workers: int = 10,
    masksql_test_path: str = None,
    masksql_match_mode: str = "auto",
    run_ri_attack: bool = False,
    ri_attack_base_url: str = None,
    ri_attack_model_name: str = None,
    ri_attack_api_key: str = "EMPTY",
):
    """
    Use Agent to perform multi-threaded concurrent inference on the dataset, extract SQL, and run tests
    """
    logging.info(f"Loading dataset: {data_path}")
    data = read_data(data_path)
    limit_rows = int(os.environ.get("LIMIT_ROWS", "0"))
    if limit_rows > 0:
        data = data[:limit_rows]
        logging.info(f"LIMIT_ROWS is set; evaluating first {len(data)} rows.")
    masksql_match_report = None
    if masksql_test_path:
        data, masksql_match_report = filter_and_attach_masksql_annotations(
            data=data,
            masksql_test_path=masksql_test_path,
            match_mode=masksql_match_mode,
        )
        logging.info(f"MaskSQL annotated subset report: {json.dumps(masksql_match_report, ensure_ascii=False)}")
    total_data = len(data)
    if total_data == 0:
        raise ValueError("No data rows selected for evaluation. Check data_path, masksql_test_path, and masksql_match_mode.")
    
    # Build output path for predictions and eval
    reasoning_file_dir = os.path.join(output_dir, inference_data_name, "agent_eval")
    os.makedirs(reasoning_file_dir, exist_ok=True)
    
    pred_sql_path = os.path.join(reasoning_file_dir, 'predict.sql')
    json_result_path = os.path.join(reasoning_file_dir, 'predict.json')
    all_res_path = os.path.join(reasoning_file_dir, 'all_res.json')
    partial_results_path = os.path.join(reasoning_file_dir, 'partial_results.json')
    privacy_eval_path = os.path.join(reasoning_file_dir, 'privacy_eval.json')
    masksql_match_report_path = os.path.join(reasoning_file_dir, 'masksql_match_report.json')

    results_by_idx = {}
    if os.path.exists(partial_results_path):
        try:
            with open(partial_results_path, 'r', encoding='utf-8') as f_partial:
                partial_results = json.load(f_partial)
            for res in partial_results:
                if isinstance(res, dict) and "idx" in res:
                    results_by_idx[int(res["idx"])] = res
            logging.info(f"Loaded {len(results_by_idx)} partial results from {partial_results_path}")
        except Exception as partial_error:
            logging.warning(f"Failed to load partial results from {partial_results_path}: {partial_error}")

    def save_partial_results():
        partial_list = [results_by_idx[i] for i in sorted(results_by_idx)]
        tmp_path = partial_results_path + ".tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f_partial:
            json.dump(partial_list, f_partial, ensure_ascii=False, indent=2)
        os.replace(tmp_path, partial_results_path)

    pending_items = [
        (idx, item)
        for idx, item in enumerate(data)
        if idx not in results_by_idx
    ]
    results_list = []
    logging.info(f"⚡ Starting multi-threaded concurrent processing, max workers: {max_workers}")
    logging.info(f"Resume status: {len(results_by_idx)} done, {len(pending_items)} pending.")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_item, idx, item, agent, total_data): idx 
            for idx, item in pending_items
        }
        for future in as_completed(futures):
            res = future.result()
            results_by_idx[int(res["idx"])] = res
            if len(results_by_idx) % 20 == 0 or len(results_by_idx) == total_data:
                save_partial_results()
           

    if results_by_idx:
        save_partial_results()
    missing_indices = [idx for idx in range(total_data) if idx not in results_by_idx]
    if missing_indices:
        raise RuntimeError(f"Missing {len(missing_indices)} results after inference, e.g. {missing_indices[:10]}")
    results_list = [results_by_idx[idx] for idx in sorted(results_by_idx)]
    results_list.sort(key=lambda x: x["idx"])

    all_res = []
    bird_eval_dic = {}
    
    with open(pred_sql_path, 'w', encoding='utf-8') as f_sql:
        for res in results_list:
            f_sql.write(res["pred_sql"] + '\n')
            # Use original BIRD dev index as predict.json key. This is required
            # when only a MaskSQL-annotated subset is evaluated.
            eval_key = str(res.get("eval_original_idx", res["idx"]))
            bird_eval_dic[eval_key] = res["pred_sql"] + '\t----- bird -----\t' + res["db_id"]
            
            # 追加新增的 final_question 和 masked_question
            all_res.append({
                'eval_original_idx': eval_key,
                'question_id': res.get("case_id"),
                'case_id': res.get("case_id"),
                'db_id': res["db_id"],
                'input': res["question"],
                'evidence': res.get("evidence", ""),
                'final_question': res["final_question"],
                'masked_question': res["masked_question"],
                'masked_schema': res.get("masked_schema", ""),
                'masked_sql': res.get("masked_sql", ""),
                'unmask_dict': res.get("unmask_dict", {}),
                'masked_terms': res.get("masked_terms", []),
                'mask_mapping': res.get("mask_mapping", {}),
                'output': res["result_text"],
                'pred_sql': res["pred_sql"],
                'gold_sql': res["gold_sql"],
                'masksql_idx': res.get("masksql_idx"),
                'masksql_match_method': res.get("masksql_match_method"),
                'orig_gold_schema_links': res.get("orig_gold_schema_links", {}),
                'orig_gold_value_links': res.get("orig_gold_value_links", {}),
                'annotated_links': res.get("annotated_links", {}),
            })

    # Save JSON
    with open(json_result_path, 'w', encoding='utf-8') as f_json:
        json.dump(bird_eval_dic, f_json, indent=4)
        
    with open(all_res_path, 'w', encoding='utf-8') as f_all:
        json.dump(all_res, f_all, ensure_ascii=False, indent=4)

    if masksql_match_report is not None:
        with open(masksql_match_report_path, 'w', encoding='utf-8') as f_match:
            json.dump(masksql_match_report, f_match, ensure_ascii=False, indent=2)

    if masksql_test_path:
        attack_client = None
        if run_ri_attack:
            if not ri_attack_base_url or not ri_attack_model_name:
                raise ValueError("run_ri_attack=True requires ri_attack_base_url and ri_attack_model_name.")
            attack_client = OpenAI(
                api_key=ri_attack_api_key,
                base_url=ri_attack_base_url,
                http_client=httpx.Client(trust_env=False),
            )
        privacy_report = evaluate_privacy_results(
            results_list=results_list,
            output_path=privacy_eval_path,
            attack_client=attack_client,
            attack_model_name=ri_attack_model_name or "",
        )
        logging.info(f"Privacy evaluation aggregate: {json.dumps(privacy_report['aggregate'], ensure_ascii=False)}")

    # Execute external script evaluation
    eval_kwargs = {
        'predicted_sql_path': json_result_path, 
        'db_root_path': db_root_path, 
        'output_dir': reasoning_file_dir, 
        'diff_json_path': diff_json_path, 
        'data_mode': data_mode, 
        'num_cpus': 8, 
        'mode_gt': 'gt', 
        'mode_predict': 'gpt', 
        'ground_truth_path': ground_truth_path, 
        'meta_time_out': 30,
        'inference_data_name': inference_data_name
    }
    if os.environ.get("SKIP_EXEC_EVAL", "0").lower() in {"1", "true", "yes"}:
        logging.info("SKIP_EXEC_EVAL is set; SQL execution evaluation is skipped.")
    else:
        eval_exec(**eval_kwargs)
    logging.info(f"✅ Evaluation completed! Results are stored in: {reasoning_file_dir}")


if __name__ == '__main__':
    # Initialization
    # BASE_URL = "[https://api.deepseek.com/v1](https://api.deepseek.com/v1)"  
    # MODEL_NAME = 'deepseek-chat'
    # API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")
    
    # BASE_URL = "[https://127api.top/v1](https://127api.top/v1)"
    # MODEL_NAME = 'gemini-3-pro'
    # API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

    # BASE_URL = "[http://127.0.0.1:2337/v1](http://127.0.0.1:2337/v1)"
    # MODEL_NAME = '/mnt/wh_intern/MXY/paper/LlamaFactory/saves/agent_v1/checkpoint-50'
    # API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

    BASE_URL = os.environ.get("AGENT_BASE_URL", "http://127.0.0.1:2337/v1")
    MODEL_NAME = os.environ.get(
        "AGENT_MODEL_NAME",
        str(_RELEASE_ROOT / 'privacy-agent/verl/output/models/qwen/Qwen3-4B-Instruct/qwen3-4b_function_rm-gsm8k-sgl-multi-w-tool-verify-n16/checkpoint-10'),
    )
    API_KEY = os.environ.get("AGENT_API_KEY", "EMPTY")

    eval_data_base_dir = os.environ.get("EVAL_DATA_BASE_DIR", str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data'))
    output_dir = os.environ.get("OUTPUT_DIR", str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/ineference/output'))
    # Evaluation dataset settings
    inference_data_name_list = [
        name.strip()
        for name in os.environ.get("INFERENCE_DATA_NAMES", "bird_dev_sc").split(",")
        if name.strip()
    ]
    masksql_test_path = os.environ.get("MASKSQL_TEST_PATH", f"{eval_data_base_dir}/masksql_test.json")
    # Matching priority: strict four-field match -> relaxed evidence-tolerant match -> question_id fallback.
    # Use "strict" if you only want (db_id, question, evidence, gold_sql) exact semantic matches.
    masksql_match_mode = os.environ.get("MASKSQL_MATCH_MODE", "auto")
    run_ri_attack = os.environ.get("RUN_RI_ATTACK", "0").lower() in {"1", "true", "yes"}
    
    # BIRD settings
    db_base_dir = os.environ.get("DB_BASE_DIR", str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1'))
    diff_json_path = os.environ.get("DIFF_JSON_PATH", '/mnt/wh_intern/MXY/paper/rl/ours/open-r1/data/bird/diff.json')
    data_mode = os.environ.get("DATA_MODE", 'dev')
    ground_truth_path = os.environ.get("GROUND_TRUTH_PATH", '/mnt/wh_intern/MXY/paper/rl/ours/CHESS/data/dev/dev.sql')
    max_workers = int(os.environ.get("MAX_WORKERS", "50"))

    # ================= Initialize Agent =================
    agent = PrivacyProtectionAgent(base_url=BASE_URL, model_name=MODEL_NAME, api_key=API_KEY)
    
    # ================= Iterate and execute evaluation =================
    for inference_data_name in inference_data_name_list:
        data_path = f"{eval_data_base_dir}/{inference_data_name}.jsonl"
        log_dir = f"{output_dir}/{inference_data_name}/case_logs"

        
        logging.info(f"====================================")
        logging.info(f"Starting to evaluate dataset: {inference_data_name}")
        
        # Trigger concurrent evaluation loop 
        run_evaluation(
            agent=agent,
            data_path=data_path,
            db_root_path=db_base_dir,
            output_dir=output_dir,
            inference_data_name=inference_data_name,
            diff_json_path=diff_json_path,
            data_mode=data_mode,
            ground_truth_path=ground_truth_path,
            max_workers=max_workers,
            masksql_test_path=masksql_test_path,
            masksql_match_mode=masksql_match_mode,
            run_ri_attack=run_ri_attack,
            # Optional RI attacker configuration. Only used when run_ri_attack=True.
            ri_attack_base_url=BASE_URL,
            ri_attack_model_name=MODEL_NAME,
            ri_attack_api_key=API_KEY,
        )
