
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[2]))

import os
import json
import logging
import threading
import math
from openai import OpenAI
from typing import Dict, List, Any, Tuple
import re
import copy
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed 
import json_repair  

from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template
from tools import generate_schema_string_from_masked

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==========================================
# Utility functions
# ==========================================
def clean_space(sql):
    sql = re.sub(r'\s*\)', ')', sql)
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
    sql = re.sub(r'\(\s*', '(', sql)
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

def safe_execute_sql(conn, sql, timeout_sec=15.0):
    cursor = conn.cursor()
    timer = threading.Timer(timeout_sec, conn.interrupt)
    timer.start()
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e).lower():
            raise TimeoutError("SQL Execution timed out")
        raise e
    finally:
        timer.cancel()
        cursor.close()

def execute_sql(predicted_sql, ground_truth, db_path, timeout_sec=20.0):
    try:
        conn = sqlite3.connect(db_path)
        predicted_sql = predicted_sql.strip().rstrip(';')
        ground_truth = ground_truth.strip().rstrip(';')
        predicted_res = safe_execute_sql(conn, predicted_sql, timeout_sec)
        ground_truth_res = safe_execute_sql(conn, ground_truth, timeout_sec)
        
        if set(predicted_res) == set(ground_truth_res):
            return 1
        return 0
    except TimeoutError:
        return 0
    except Exception as e:
        return 0
    finally:
        if 'conn' in locals():
            conn.close()

def read_data(path, encoding="utf-8"):
    if path.endswith('.jsonl'):
        with open(path, "r", encoding=encoding) as f:
            data = [json.loads(line) for line in f if line.strip()]
    else:
        with open(path, "r", encoding=encoding) as f:
            data = json.load(f)
    return sorted(data, key=lambda x: int(x.get('question_id', 0)))

# ==========================================
# MCTS Node Class
# ==========================================
class MCTSNode:
    def __init__(self, parent, state_messages: List[Dict]):
        self.parent = parent
        self.state_messages = copy.deepcopy(state_messages)  # 存储走到当前节点的完整对话历史
        self.children = []
        self.visits = 0
        self.value = 0.0

    def uct(self, c=1.414):
        if self.visits == 0:
            return float('inf')
        # UCB1 公式，平衡探索与利用
        return (self.value / self.visits) + c * math.sqrt(math.log(self.parent.visits) / self.visits)


# ==========================================
# MCTS Agent Class
# ==========================================
class MCTSPrivacyProtectionAgent:
    def __init__(self, base_url: str, model_name: str, api_key: str = "EMPTY"):
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model_name = model_name
        self.system_prompt = """
You are an NL2SQL privacy protection agent. You need to use the ReAct (Reasoning and Acting) paradigm to complete the privacy desensitization task for natural language queries. You must ensure that the desensitized natural language query still allows the model to write the correct SQL query.
Under this paradigm, you must alternate between Thought and Action, and after each action, proceed to the next step based on the received Observation, until reaching a final conclusion.

Sensitive Data Definition:
Any entity related to database tables, columns, or values present in the natural language query is treated as sensitive data.

[Processing Flow and Chain of Thought]
Please strictly follow these steps:
1. Evaluate Structure: Analyze the user's original Query to evaluate whether its privacy entities can be directly replaced. If a direct replacement destroys the syntactic structure, please call the `query_rewrite` tool to rewrite it.
2. Extract Entities: Using the finally decided Query, call the `entity_extract` tool to extract all privacy entities.
3. Masking Operation: After obtaining the entity list, call the `apply_typed_masking` tool to mask the schema and the user's question respectively.
4. Evaluate Masking: Evaluate whether the masked query and schema are reasonable.
5. Generate SQL: Pass the masked query and schema to `sql_generate` to generate the desensitized SQL.
6. Unmask SQL: After getting the generated desensitized SQL, call the `sql_unmask` tool, passing in the SQL and the reverse mapping dictionary (`unmask_dict`) to restore the real SQL.
7. Verify and Repair: Call the `execute_sql_check` tool to test the restored real SQL on the database. 
   - If it returns "Success", the task is complete.
   - If it returns an error, analyze the raw database error message, fix the real SQL directly yourself, and call `execute_sql_check` again with your repaired real SQL. Do NOT go back to `sql_generate` or masking steps for repairing.

[Output Requirements]
Before deciding to call any tool, you must first output your thought process in your answer! Each response should follow this logic:
Thought: [Write down your thought process here. Analyze the current input, the previous Observation, what next step you need to take, and why.]
Action: [Execute your action by triggering the corresponding function call]

After the tool executes, the system will return the result to you as an Observation.

When you have completed all steps and `execute_sql_check` returns "Success":
Thought: [State that I have obtained the verified real SQL and the task is complete.]
Directly output the final real SQL as the final response, without calling the tool again. You need to place the final SQL in the ```sql\n\n``` markdown format.
"""
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "query_rewrite",
                    "description": "Call this tool when the original query formulation is irregular.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query_to_rewrite": {"type": "string"},
                            "rewrite_instruction": {"type": "string"}
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
                    "description": "Simultaneously perform co-frequency table, column, and value masking replacement.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "entities": {"type": "array", "items": {"type": "object"}}
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
                            "masked_query": {"type": "string"},
                            "masked_schema_str": {"type": "string"}
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
                            "masked_sql": {"type": "string"},
                            "unmask_dict": {"type": "object"}
                        },
                        "required": ["masked_sql", "unmask_dict"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "execute_sql_check",
                    "description": "Execute the unmasked real SQL statement on the database to check for syntax or logical errors. Returns 'Success' if valid, or a raw database error message if invalid. Use this to verify and repair your SQL.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "unmasked_sql": {"type": "string"}
                        },
                        "required": ["unmasked_sql"]
                    }
                }
            }
        ]


    # --- Tool implementations (此处保持你原来的工具实现不变) ---
    def _tool_query_rewrite(self, db_schema, query_to_rewrite, rewrite_instruction="", logger=None):
        rewrite_client = OpenAI(api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"), base_url="http://127.0.0.1:2335/v1")
        model_name = str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-25')
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_rewrite}"
        if rewrite_instruction: input_str += f"\n- Rewrite Instruction:\n{rewrite_instruction}"
        response = rewrite_client.chat.completions.create(model=model_name, messages=get_query_rewrite_messages(input_str), temperature=0)
        return response.choices[0].message.content.strip()

    def _tool_entity_extract(self, db_schema, query_to_extract, logger=None):
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_extract}"
        entity_extract_client = OpenAI(api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"), base_url="http://127.0.0.1:2336/v1")
        model_name = str(_RELEASE_ROOT / 'LlamaFactory/saves/entity_extract_v1/checkpoint-25')
        response = entity_extract_client.chat.completions.create(model=model_name, messages=get_entity_extract_template(input_str), temperature=0)
        match = re.search(r'\[\s*\{.*?\}\s*\]', response.choices[0].message.content.strip(), re.DOTALL)
        return match.group(0) if match else "[]"
    
    def _tool_sql_generate(self, masked_query, masked_schema_str, logger=None):
        input_str = f"- Database Schema:\n{masked_schema_str}\n- User's Question:\n{masked_query}"
        response = self.client.chat.completions.create(model=self.model_name, messages=get_sql_generate_template(input_str), temperature=0)
        return response.choices[0].message.content.strip()

    def _tool_sql_unmask(self, masked_sql: str, unmask_dict: Any) -> str:
        if isinstance(unmask_dict, str):
            try: unmask_dict = json_repair.loads(unmask_dict)
            except Exception: pass
        if not masked_sql or not isinstance(unmask_dict, dict): return masked_sql
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
                    if left_char == ".": return col_part
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
        # print(unmasked_sql)
        # print(db_path)
        if not unmasked_sql or not db_path: return "Error: Missing SQL or Database Path."
        try:
            conn = sqlite3.connect(db_path)
            check_sql = f"SELECT * FROM ({unmasked_sql.strip().rstrip(';')}) LIMIT 1;"
            safe_execute_sql(conn, check_sql, timeout_sec=15.0)
            return "Success"
        except TimeoutError:
            return "SQL Execution Error: Execution timed out."
        except Exception as e:
            return f"SQL Execution Error: {str(e)}"
        finally:
            if 'conn' in locals(): conn.close()

    

    def _tool_apply_typed_masking(self, query, entities, structured_db_schema):
        table_map = {}
        col_map = {}
        val_map = {}
        
        t_idx = 1
        c_idx = 1
        v_idx = 1
        
        for table_name, table_info in structured_db_schema.items():
            if not table_name or not str(table_name).strip(): 
                continue 
            
            if table_name not in table_map:
                table_map[table_name] = f"table_{t_idx}"
                t_idx += 1
                
            if "columns" in table_info:
                cols = table_info["columns"]
            else:
                cols = [{"column_name": k, **v} for k, v in table_info.items()]
                
            for col_info in cols:
                col_name = col_info.get("column_name", "")
                if not col_name or not str(col_name).strip(): 
                    continue 
                
                if (table_name, col_name) not in col_map:
                    col_map[(table_name, col_name)] = f"col_{c_idx}"
                    c_idx += 1

        for ent in entities:
            if ent.get("entity_type") == "cell_value" and ent.get("cell_value") is not None:
                val_str = str(ent["cell_value"])
                if not val_str.strip(): 
                    continue
                    
                if val_str not in val_map:
                    val_map[val_str] = f"val_{v_idx}"
                    v_idx += 1

        def replace_in_string(text):
            if not isinstance(text, str) or not text: 
                return text
            replace_dict = {orig: mask for orig, mask in table_map.items()}
            replace_dict.update({orig_c: mask for (orig_t, orig_c), mask in col_map.items()})
            replace_dict.update({orig: mask for orig, mask in val_map.items()})
            
            sorted_keys = sorted(replace_dict.keys(), key=lambda k: len(str(k)), reverse=True)
            res = text
            for k in sorted_keys:
                k_str = str(k)
                if not k_str: 
                    continue
                
                if re.match(r'^\w+$', k_str):
                    res = re.sub(r'\b' + re.escape(k_str) + r'\b', replace_dict[k], res, flags=re.IGNORECASE)
                else:
                    res = res.replace(k_str, replace_dict[k])
            return res

        masked_query = query
        sorted_entities = sorted(entities, key=lambda x: len(str(x.get("entity", ""))), reverse=True)
        
        for ent in sorted_entities:
            ent_str = str(ent.get("entity", ""))
            if not ent_str.strip():
                continue
                
            mask = None
            if ent.get("entity_type") == "table":
                mask = table_map.get(ent.get("table"))
            elif ent.get("entity_type") == "column":
                mask = col_map.get((ent.get("table"), ent.get("column")))
            elif ent.get("entity_type") == "cell_value":
                mask = val_map.get(str(ent.get("cell_value")))
                
            if mask:
                if re.match(r'^\w+$', ent_str):
                    pattern = re.compile(r'\b' + re.escape(ent_str) + r'\b', re.IGNORECASE)
                else:
                    pattern = re.compile(re.escape(ent_str), re.IGNORECASE)
                masked_query = pattern.sub(mask, masked_query)
                
        masked_query = replace_in_string(masked_query)

        masked_schema = {}
        for table_name, table_info in structured_db_schema.items():
            if not table_name or not str(table_name).strip(): 
                continue
            
            m_table_name = table_map.get(table_name, table_name)
            m_table_info = {}
            
            if "columns" in table_info:
                m_table_info["columns"] = []
                for col_info in table_info["columns"]:
                    m_col_info = {}
                    for attr_key, attr_val in col_info.items():
                        if attr_key == "examples":
                            continue 
                            
                        if isinstance(attr_val, str):
                            m_col_info[attr_key] = replace_in_string(attr_val)
                        else:
                            m_col_info[attr_key] = attr_val
                            
                    orig_col_name = col_info.get("column_name", "")
                    m_col_info["column_name"] = col_map.get((table_name, orig_col_name), orig_col_name)
                    m_table_info["columns"].append(m_col_info)
                    
                for k, v in table_info.items():
                    if k not in ["columns", "examples"]: 
                        if isinstance(v, list):
                            m_table_info[k] = [replace_in_string(item) if isinstance(item, str) else item for item in v]
                        elif isinstance(v, str):
                            m_table_info[k] = replace_in_string(v)
                        else:
                            m_table_info[k] = v
            else:
                for col_name, col_attrs in table_info.items():
                    m_col_name = col_map.get((table_name, col_name), col_name)
                    m_col_attrs = {}
                    for attr_key, attr_val in col_attrs.items():
                        if attr_key == "examples": continue
                        if isinstance(attr_val, str):
                            m_col_attrs[attr_key] = replace_in_string(attr_val)
                        else:
                            m_col_attrs[attr_key] = attr_val
                    m_table_info[m_col_name] = m_col_attrs
                    
            masked_schema[m_table_name] = m_table_info

        unmask_dict = {
            "tables": {mask: orig for orig, mask in table_map.items()},
            "columns": {mask: f"{orig_t}.{orig_c}" for (orig_t, orig_c), mask in col_map.items()},
            "values": {mask: orig for orig, mask in val_map.items()}
        }
        masked_schema_str = generate_schema_string_from_masked(masked_schema)
        
        return {
            "masked_query": masked_query, 
            "masked_schema_str": masked_schema_str, 
            "unmask_dict": unmask_dict
        }

    # ==========================================
    # MCTS Core Modules (🚀 已修复对象不可被索引的问题)
    # ==========================================
    def _step_forward(self, messages_history: List[Dict], db_schema: str, structured_db_schema: Dict, db_path: str, temp: float, logger=None):
        try:
            response = self.client.chat.completions.create(
                model=self.model_name, messages=messages_history, 
                tools=self.tools, tool_choice="auto", temperature=temp
            )
            message = response.choices[0].message
        except Exception as e:
            return [], True, False, "", {}

        parsed_tool_calls = []
        has_tool_error = False
        is_final = False
        final_sql = ""
        content = message.content or ""

        # 解析兜底：有些模型会输出 XML 格式的 tool_call
        if content:
            matches = re.finditer(r'<tool_call>\s*(.*?)\s*</tool_call>', content, re.DOTALL)
            for match_idx, match in enumerate(matches):
                try:
                    tool_data = json_repair.loads(match.group(1))
                    parsed_tool_calls.append({"name": tool_data.get("name"), "arguments": tool_data.get("arguments", {}), "id": f"call_{match_idx}"})
                except json.JSONDecodeError: 
                    has_tool_error = True 

        # 构建纯净的 dict 格式存入列表（修复 Object is not subscriptable 报错）
        assistant_msg = {"role": "assistant", "content": content}
        new_messages = []

        if message.tool_calls or parsed_tool_calls:
            try:
                calls_to_process = [{"name": tc.function.name, "arguments": json_repair.loads(tc.function.arguments) if isinstance(tc.function.arguments, str) else tc.function.arguments, "id": tc.id} for tc in message.tool_calls] if message.tool_calls else parsed_tool_calls
            except Exception:
                calls_to_process = []
                has_tool_error = True

            # 转换为 LlamaFactory 所需的格式
            assistant_msg["tool_calls"] = [
                {
                    "id": tc["id"], "type": "function",
                    "function": {"name": tc["name"], "arguments": json.dumps(tc["arguments"], ensure_ascii=False) if isinstance(tc["arguments"], dict) else tc["arguments"]}
                } for tc in calls_to_process
            ]
            new_messages.append(assistant_msg) # 加入大模型回复

            # 遍历执行所有工具
            for tc_data in calls_to_process:
                func_name, args, tool_call_id = tc_data["name"], tc_data["arguments"], tc_data["id"]
                try:
                    if func_name == "query_rewrite": result = self._tool_query_rewrite(db_schema, args.get('query_to_rewrite', ''), args.get('rewrite_instruction', ''))
                    elif func_name == "entity_extract": result = self._tool_entity_extract(db_schema, args.get('query_to_extract', ''))
                    elif func_name == "apply_typed_masking": result = self._tool_apply_typed_masking(args.get('query', ''), args.get('entities', []), structured_db_schema)
                    elif func_name == "sql_generate": result = self._tool_sql_generate(args.get('masked_query', ''), args.get('masked_schema_str', ''))
                    elif func_name == "sql_unmask": result = self._tool_sql_unmask(args.get('masked_sql', ''), args.get('unmask_dict', {}))
                    elif func_name == "execute_sql_check": result = self._tool_execute_sql_check(args.get('unmasked_sql', ''), db_path)
                    else: 
                        result = f"Error: Unknown tool {func_name}"
                        has_tool_error = True 
                except Exception as e:
                    result = f"Error executing {func_name}: {str(e)}"
                    has_tool_error = True 
                
                if isinstance(result, str) and result.startswith("Error executing"):
                    has_tool_error = True

                # 加入工具执行结果 (同样使用纯净的 dict 格式)
                new_messages.append({"role": "tool", "tool_call_id": tool_call_id, "name": func_name, "content": str(result)})
        else:
            new_messages.append(assistant_msg)
            is_final = True
            final_sql = find_result(content)

        # 这里返回的 new_messages 是一个完全由 dict 组成的列表，不会再引发 TypeError
        return new_messages, has_tool_error, is_final, final_sql, assistant_msg

    def rollout(self, start_node: MCTSNode, item: Dict, db_schema: str, structured_db_schema: Dict, db_path: str, max_depth: int = 6) -> Tuple[int, List[Dict]]:
        current_messages = copy.deepcopy(start_node.state_messages)
        new_train_messages = []

        for _ in range(max_depth):
            new_msgs, has_error, is_final, final_sql, ast_msg_train = self._step_forward(
                current_messages, db_schema, structured_db_schema, db_path, temp=0.0
            )
            
            if has_error:
                return 0, []
            
            new_train_messages.append(ast_msg_train)
            current_messages.extend(new_msgs)

            # msg 都是标准的 dict 对象，直接使用 get("role")
            for msg in new_msgs:
                if msg.get("role") == "tool":
                    new_train_messages.append({"role": "tool", "tool_call_id": msg["tool_call_id"], "name": msg["name"], "content": msg["content"]})

            if is_final:
                if final_sql and item['gold_sql']:
                    if execute_sql(final_sql, item['gold_sql'], db_path) == 1:
                        return 1, new_train_messages
                return 0, []

        return 0, []

    def mcts_process(self, item: Dict[str, Any], case_id: str = "default", db_path: str = "", max_iters: int = 4, branch_factor: int = 3) -> Tuple[str, List[Dict[str, Any]], bool]:
        original_question = item['question']
        db_schema = item.get('db_schema', '') 
        evidence = item.get("evidence", '')
        structured_db_schema = item.get('structured_db_schema', {}) 
        
        user_prompt = f"- Database Schema:\n{db_schema}\n\n- User Question:\n{original_question} {evidence}\n\nTask started. Please provide your analysis (Thought) and call the tools (Action)."

        base_messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        root = MCTSNode(parent=None, state_messages=base_messages)
        root.train_history = copy.deepcopy(base_messages) 

        for iter_idx in range(max_iters):
            node = root
            while len(node.children) == branch_factor: 
                node = max(node.children, key=lambda c: c.uct())
            
            for _ in range(branch_factor):
                new_msgs, has_error, is_final, final_sql, ast_msg_train = self._step_forward(
                    node.state_messages, db_schema, structured_db_schema, db_path, temp=0.7
                )
                
                if has_error: continue
                
                child_state = copy.deepcopy(node.state_messages)
                child_state.extend(new_msgs)
                
                child_train_history = copy.deepcopy(node.train_history)
                child_train_history.append(ast_msg_train)
                
                # msg 都是标准的 dict 对象，直接使用 get("role")
                for m in new_msgs:
                    if m.get("role") == "tool":
                        child_train_history.append({"role": "tool", "tool_call_id": m["tool_call_id"], "name": m["name"], "content": m["content"]})
                
                child_node = MCTSNode(parent=node, state_messages=child_state)
                child_node.train_history = child_train_history
                node.children.append(child_node)

                if is_final:
                    reward = 1 if execute_sql(final_sql, item['gold_sql'], db_path) == 1 else 0
                    rollout_train_path = []
                else:
                    reward, rollout_train_path = self.rollout(child_node, item, db_schema, structured_db_schema, db_path)
                
                if reward == 1:
                    logging.info(f"[{case_id}] 🌟 Golden Trajectory Found during MCTS Iter {iter_idx+1}!")
                    final_training_messages = child_node.train_history + rollout_train_path
                    final_text = final_training_messages[-1].get("content", "") if final_training_messages[-1].get("role") == "assistant" else "Success"
                    return final_text, final_training_messages, False
                
                curr = child_node
                while curr is not None:
                    curr.visits += 1
                    curr.value += reward
                    curr = curr.parent

        logging.warning(f"[{case_id}] ❌ MCTS exhausted max iterations without finding a flawless trajectory.")
        return '{"error": "MCTS exhausted without success"}', [], True
    

# ==========================================
# Data Generation Thread Logic
# ==========================================
def process_single_item(idx, item, agent: MCTSPrivacyProtectionAgent, total_num, db_base_dir, cache_dir):
    case_id = str(item.get("question_id", f"idx_{idx}"))
    db_id = item.get('db_id', 'unknown_db')
    gold_sql = item.get('gold_sql', '')
    
    cache_file = os.path.join(cache_dir, f"{case_id}.json")
    if os.path.exists(cache_file):
        logging.info(f"⏭️ Cache found for case [{idx+1}/{total_num}] - {case_id}, skipping.")
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    db_path = f"{db_base_dir}/{db_id}/{db_id}.sqlite"
    
    try:
        # 使用 MCTS 进行推理合成
        result_text, training_messages, has_tool_error = agent.mcts_process(item, case_id=case_id, db_path=db_path)
        pred_sql = find_result(result_text)
        
        # MCTS 如果返回的 has_tool_error 是 False 且 messages 不空，代表一定找到了正确的路径。
        # 这里仅作最后的保险校验。
        is_correct = 0
        if pred_sql and gold_sql and not has_tool_error:
            is_correct = execute_sql(pred_sql, gold_sql, db_path, timeout_sec=20.0)
        
        # 终极过滤：只保存 100% 对的数据
        if not is_correct or has_tool_error:
            training_messages = []
            
        logging.info(f"✅ Case [{idx+1}/{total_num}] - {case_id} finished. Correct: {bool(is_correct)}")
    except Exception as e:
        logging.error(f"❌ Failed case [{idx+1}/{total_num}] - {case_id}: {str(e)}")
        result_text = f"Error: {str(e)}"
        pred_sql, training_messages = "", []
        is_correct, has_tool_error = 0, True

    res_data = {
        "idx": idx, "case_id": case_id, "db_id": db_id,
        "question": item.get('question', ''), "gold_sql": gold_sql,
        "result_text": result_text, "pred_sql": pred_sql,
        "training_messages": training_messages,
        "is_correct": is_correct, "has_tool_error": has_tool_error
    }
    
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(res_data, f, ensure_ascii=False, indent=2)
        
    return res_data


def run_generation(agent, data_path, db_root_path, output_dir, inference_data_name, train_data_dir, max_workers=20):
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
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_item, idx, item, agent, total_data, db_root_path, cache_dir): idx for idx, item in enumerate(data)}
        for future in as_completed(futures):
            try:
                results_list.append(future.result())
            except Exception as e:
                logging.error(f"Thread error: {str(e)}")

    results_list.sort(key=lambda x: x["idx"])

    all_res, bird_eval_dic = [], {}
    
    # 写入结果，只写入符合条件的训练数据
    with open(pred_sql_path, 'w', encoding='utf-8') as f_sql, open(training_data_path, 'w', encoding='utf-8') as f_train:
        for res in results_list:
            f_sql.write(res["pred_sql"] + '\n')
            bird_eval_dic[res["idx"]] = res["pred_sql"] + '\t----- bird -----\t' + res["db_id"]
            
            all_res.append({
                'db_id': res["db_id"], 'input': res["question"],
                'output': res["result_text"], 'pred_sql': res["pred_sql"],
                'gold_sql': res["gold_sql"], 'is_correct': res.get("is_correct", 0),
                'has_tool_error': res.get("has_tool_error", False)
            })

            # 如果 training_messages 不为空，说明通过了终极考验（0错误+正确SQL）
            if res.get("training_messages"):
                f_train.write(json.dumps({"messages": res["training_messages"]}, ensure_ascii=False) + '\n')

    with open(json_result_path, 'w', encoding='utf-8') as f_json: json.dump(bird_eval_dic, f_json, indent=4)
    with open(all_res_path, 'w', encoding='utf-8') as f_all: json.dump(all_res, f_all, indent=4)

    logging.info(f"✅ Data synthesis completed! High quality training data saved in: {training_data_path}")

if __name__ == '__main__':
    # 你的运行配置
    # BASE_URL = "https://api.deepseek.com/v1"  
    # MODEL_NAME = 'deepseek-chat'
    # API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY") # 建议正式环境用环境变量读取
    
    BASE_URL = "http://127.0.0.1:8001/v1"
    MODEL_NAME = 'Qwen3.5-122B-A10B'
    API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")
    
    eval_data_base_dir = str(_RELEASE_ROOT / 'privacy-agent/agent/data')
    output_dir = str(_RELEASE_ROOT / 'privacy-agent/agent/output')
    train_data_dir = str(_RELEASE_ROOT / 'privacy-agent/agent/train_data')
    db_base_dir = str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1')
    inference_data_name_list = ['bird_train_sc_filtered']

    # 实例化新的 MCTS Agent
    agent = MCTSPrivacyProtectionAgent(base_url=BASE_URL, model_name=MODEL_NAME, api_key=API_KEY)
    
    for inference_data_name in inference_data_name_list:
        run_generation(
            agent=agent,
            data_path=f"{eval_data_base_dir}/{inference_data_name}.jsonl",
            db_root_path=db_base_dir,
            output_dir=output_dir,
            inference_data_name=inference_data_name,
            train_data_dir=train_data_dir,
            max_workers=1 # 建议根据 API 限流情况适当调低(如20)
        )