
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[2]))

import os
import json
import logging
import math
import re
import copy
import sqlite3
from openai import OpenAI
from typing import Dict, List, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed 
import json_repair  

# ==========================================
# 导入你的本地依赖 (请确保这些路径与你本地一致)
# ==========================================
from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template
from tools import generate_schema_string_from_masked, execute_sql, find_result, safe_execute_sql

# 引入各个步骤的示例数据
from examples.evaluate_mask_en import evaluate_mask_messages
from examples.evaluate_structure_en import evaluate_structure_messages

# 配置日志
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
# MCTS 核心树节点定义
# ==========================================
class MCTSNode:
    def __init__(self, messages, parent=None, action_msg=None, tool_result_msg=None):
        self.messages = copy.deepcopy(messages)  # 到达此节点时的完整对话上下文
        self.parent = parent
        self.children = []
        
        self.visits = 0      # 访问次数 N
        self.value = 0.0     # 累计价值 W
        
        self.action_msg = action_msg          # LLM 产生的动作 (assistant message)
        self.tool_result_msg = tool_result_msg # 环境返回的观察 (tool message)
        
        self.is_terminal = False
        self.pred_sql = None
        
        # 记录该节点所使用的脱敏数据状态，用于最后算分
        self.current_masked_query = ""
        self.current_masked_schema = ""

    def ucb1(self, exploration_weight=1.414):
        if self.visits == 0:
            return float('inf')
        # UCB1 公式: (W / N) + c * sqrt(ln(N_parent) / N)
        exploitation = self.value / self.visits
        # 防止根节点报错
        parent_visits = self.parent.visits if self.parent and self.parent.visits > 0 else 1
        exploration = exploration_weight * math.sqrt(math.log(parent_visits) / self.visits)
        return exploitation + exploration

# ==========================================
# 隐私保护 Agent 类 (包含 MCTS 逻辑)
# ==========================================
class PrivacyProtectionAgent:
    def __init__(self, base_url: str, model_name: str, api_key: str = "EMPTY"):
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model_name = model_name
        self.temperature = 0 # 基础温度，MCTS 扩展时会临时调高

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


    # ==========================================
    # MCTS: 隐私评估函数 (上帝视角 Oracle)
    # ==========================================
# ==========================================
    # MCTS: 隐私评估函数 (LLM-as-a-Judge)
    # ==========================================
    def calculate_privacy_reward(self, original_query: str, masked_query: str, masked_schema_str: str, structured_db_schema: dict, gold_sql: str) -> float:
        if not masked_query or not masked_schema_str:
            return 0.0 # 连脱敏结果都没有，直接 0 分

        # 构建裁判模型的 Prompt
        judge_prompt = f"""You are an expert Data Privacy Evaluator. Your task is to evaluate whether a natural language query and a database schema have been properly desensitized (masked), ensuring no real sensitive database entities are leaked.

[Inputs]
1. Original User Query: "{original_query}"
2. Gold SQL (True database execution logic): "{gold_sql}"
3. Masked Query (To be evaluated): "{masked_query}"
4. Masked Schema (To be evaluated): "{masked_schema_str}"

[Evaluation Rules]
1. Identify true sensitive entities: Look at the Original Query and Gold SQL. Identify specific table names, column names, and real data values.
2. Check for leaks: Carefully inspect the Masked Query and Masked Schema. Does the Masked Query or Masked Schema still contain the real table names, column names, or real data values explicitly mentioned in the inputs?
3. Synonyms & Semantics: Pay attention to semantic leaks. If "Alice" is masked but the surrounding context explicitly gives away her identity inappropriately, it's a leak. However, standard SQL keywords (SELECT, WHERE, etc.) and generic natural language words are NOT sensitive.

[Scoring Criteria]
- Perfect (1.0): No sensitive entities from the original data are leaked in the masked query and masked schema.
- Partial/Poor (< 1.0): Deduct 0.2 points for EVERY distinct sensitive entity that is leaked (e.g., if the real table name and a real person's name are both leaked, deduct 0.4. Score = 0.6).
- Minimum score is 0.0.

[Output Format]
You MUST return ONLY a valid JSON object. Do not output markdown code blocks like ```json.
The JSON must have exactly these two keys:
- "thought": A brief string explaining your step-by-step analysis.
- "score": A float number between 0.0 and 1.0.

Example Output:
{{"thought": "The true entity 'Alice' was replaced with 'val_1', but the true column name 'salary' is still explicitly present in the masked query. Deducting 0.2.", "score": 0.8}}
"""
        
        try:
            # 调用大模型进行打分 (为了速度和成本，建议这里的 temperature 设为 0)
            response = self.client.chat.completions.create(
                model=self.model_name, # 如果有更便宜/快的模型(如 deepseek-chat)，也可以在这里硬编码替换
                messages=[
                    {"role": "system", "content": "You are a strict and objective json-output evaluator."},
                    {"role": "user", "content": judge_prompt}
                ],
                temperature=0.0,
                # 如果使用的模型支持强制 JSON 模式，可以加上下面这行；不支持则注释掉
                response_format={"type": "json_object"} 
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # 解析 JSON 结果
            import json_repair # 借助你已经引入的库增加容错性
            parsed_result = json_repair.loads(result_text)
            
            score = float(parsed_result.get("score", 0.0))
            thought = parsed_result.get("thought", "No thought provided")
            
            # 限制分数范围
            score = max(0.0, min(1.0, score))
            
            logging.info(f"⚖️ [LLM Judge] 评分: {score:.2f} | 理由: {thought}")
            return score
            
        except Exception as e:
            logging.error(f"⚠️ [LLM Judge] 调用或解析失败: {str(e)}。回退分数为 0.0")
            return 0.0
    # ==========================================
    # MCTS: 模拟器 (执行工具并返回结果)
    # ==========================================
    def _execute_tool_for_mcts(self, action_msg, db_schema, structured_db_schema, db_path):
        """执行 LLM 生成的工具调用，返回工具观察结果以及拦截的中间变量"""
        tool_call = action_msg.tool_calls[0] if getattr(action_msg, 'tool_calls', None) else None
        if not tool_call:
            return None, "", ""
            
        func_name = tool_call.function.name
        try:
            args = json_repair.loads(tool_call.function.arguments) if isinstance(tool_call.function.arguments, str) else tool_call.function.arguments
        except Exception:
            return {"role": "tool", "tool_call_id": tool_call.id, "name": func_name, "content": "Error: JSON parse failed"}, "", ""

        masked_q, masked_s = "", ""
        try:
            if func_name == "query_rewrite": 
                result = self._tool_query_rewrite(db_schema, args.get('query_to_rewrite', ''), args.get('rewrite_instruction', ''))
            elif func_name == "entity_extract": 
                result = self._tool_entity_extract(db_schema, args.get('query_to_extract', ''))
            elif func_name == "apply_typed_masking": 
                res_dict = self._tool_apply_typed_masking(args.get('query', ''), args.get('entities', []), structured_db_schema)
                result = str(res_dict) # 注意这里需要把字典转为字符串作为大模型的观察结果
            elif func_name == "sql_generate":
                masked_s = args.get('masked_schema_str', '')
                masked_q = args.get('masked_query', '')
                result = self._tool_sql_generate(masked_q, masked_s)
            elif func_name == "sql_unmask":
                result = self._tool_sql_unmask(args.get('masked_sql', ''), args.get('unmask_dict', {}))
            elif func_name == "execute_sql_check":
                result = self._tool_execute_sql_check(args.get('unmasked_sql', ''), db_path)
            else: 
                result = f"Error: Unknown tool {func_name}"
        except Exception as e:
            result = f"Error executing {func_name}: {str(e)}"
            
        tool_msg = {"role": "tool", "tool_call_id": tool_call.id, "name": func_name, "content": str(result)}
        return tool_msg, masked_q, masked_s

    # ==========================================
    # MCTS: 核心主循环
    # ==========================================
    def process_mcts(self, item: Dict[str, Any], case_id: str, db_path: str, num_simulations: int = 5, expansion_width: int = 3) -> Tuple[str, List[Dict[str, Any]], bool]:
        original_question = item['question']
        db_schema = item.get('db_schema', '') 
        gold_sql = item.get('gold_sql', '')
        structured_db_schema = item.get('structured_db_schema', {}) 
        
        user_prompt = f"- Database Schema:\n{db_schema}\n\n- User Question:\n{original_question}\n\nTask started. Please provide your analysis (Thought) and call the tools (Action)."
        
        initial_messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        root_node = MCTSNode(messages=initial_messages)
        best_pred_sql = ""
        best_score = -1.0
        best_trajectory = []

        logging.info(f"🌳 [MCTS] 启动: Case {case_id}, 计划模拟 {num_simulations} 次, 探索宽度 {expansion_width}")

        for sim in range(num_simulations):
            # 1. Selection (选择阶段)
            current_node = root_node
            while current_node.children and len(current_node.children) > 0:
                unvisited = [c for c in current_node.children if c.visits == 0]
                if unvisited:
                    current_node = unvisited[0]
                    break
                current_node = max(current_node.children, key=lambda c: c.ucb1())
                
            # 2. Expansion (扩展阶段)
            if not current_node.is_terminal and (current_node.visits > 0 or current_node == root_node):
                try:
                    # 使用高温度生成多种可能
                    response = self.client.chat.completions.create(
                        model=self.model_name,
                        messages=current_node.messages,
                        tools=self.tools,
                        tool_choice="auto",
                        temperature=0.8, 
                        n=expansion_width,
                    )
                    
                    seen_tool_calls = set()
                    for choice in response.choices:
                        action_msg = choice.message
                        is_terminal = False
                        pred_sql = None
                        
                        if not getattr(action_msg, 'tool_calls', None):
                            pred_sql_match = find_result(action_msg.content or "")
                            if pred_sql_match:
                                is_terminal = True
                                pred_sql = pred_sql_match
                            else:
                                continue # 无效节点跳过
                        
                        # 简单的去重机制
                        sig = str(action_msg.tool_calls[0].function.arguments) if not is_terminal else str(action_msg.content)
                        if sig in seen_tool_calls:
                            continue
                        seen_tool_calls.add(sig)
                        
                        if not is_terminal:
                            tool_msg, masked_q, masked_s = self._execute_tool_for_mcts(action_msg, db_schema, structured_db_schema, db_path)
                            new_messages = current_node.messages + [action_msg, tool_msg]
                        else:
                            new_messages = current_node.messages + [action_msg]
                            tool_msg, masked_q, masked_s = None, "", ""
                            
                        child = MCTSNode(messages=new_messages, parent=current_node, action_msg=action_msg, tool_result_msg=tool_msg)
                        child.is_terminal = is_terminal
                        child.pred_sql = pred_sql
                        child.current_masked_query = masked_q if masked_q else current_node.current_masked_query
                        child.current_masked_schema = masked_s if masked_s else current_node.current_masked_schema
                        
                        current_node.children.append(child)
                        
                    if current_node.children:
                        current_node = current_node.children[0]
                except Exception as e:
                    logging.error(f"[MCTS] 扩展失败: {e}")
                    current_node.is_terminal = True

            # 3. Simulation (Rollout 阶段)
            rollout_node = current_node
            depth = 0
            while not rollout_node.is_terminal and depth < 16: # 限制最大深度防止死循环
                try:
                    # Rollout 阶段使用贪婪策略
                    response = self.client.chat.completions.create(
                        model=self.model_name, messages=rollout_node.messages, tools=self.tools, temperature=0.0
                    )
                    action_msg = response.choices[0].message
                    
                    if not getattr(action_msg, 'tool_calls', None):
                        pred_sql_match = find_result(action_msg.content or "")
                        rollout_node.is_terminal = True
                        rollout_node.pred_sql = pred_sql_match if pred_sql_match else ""
                        break
                        
                    tool_msg, masked_q, masked_s = self._execute_tool_for_mcts(action_msg, db_schema, structured_db_schema, db_path)
                    
                    rollout_node.messages.extend([action_msg, tool_msg])
                    if masked_q: rollout_node.current_masked_query = masked_q
                    if masked_s: rollout_node.current_masked_schema = masked_s
                    depth += 1
                except Exception as e:
                    logging.error(f"[MCTS] Rollout 报错: {e}")
                    break

            # 4. Evaluation & Backpropagation (反向传播阶段)
            reward = 0.0
            if rollout_node.pred_sql:
                is_correct = execute_sql(rollout_node.pred_sql, gold_sql, db_path, logging, timeout_sec=15.0)
                sql_reward = 1.0 if is_correct else 0.0
                
                privacy_reward = self.calculate_privacy_reward(
                    original_question, 
                    rollout_node.current_masked_query, 
                    rollout_node.current_masked_schema, 
                    structured_db_schema, 
                    gold_sql
                )
                
                reward = sql_reward * privacy_reward
                
                if reward > best_score:
                    best_score = reward
                    best_pred_sql = rollout_node.pred_sql
                    best_trajectory = rollout_node.messages

                logging.info(f"🔄 [Sim {sim+1}/{num_simulations}] 路径得分: {reward:.2f} (SQL正确: {is_correct}, 隐私: {privacy_reward:.2f})")

            # 更新树结构状态
            back_node = current_node
            while back_node is not None:
                back_node.visits += 1
                back_node.value += reward
                back_node = back_node.parent

        logging.info(f"🏆 [MCTS 完成] Case {case_id} 最好得分: {best_score:.2f}")
        
        # has_tool_error = best_score == 0.0 
        is_invalid_for_training = best_score < 0.99
        
        # 将 trajectory 转换为用于训练的格式
        training_messages = []
        for msg in best_trajectory:
            # 如果是字典（比如 system 提示词、user 输入、或者我们自己组装的 tool 结果）
            if isinstance(msg, dict):
                training_messages.append(msg)
            # 如果是模型返回的 ChatCompletionMessage 对象
            else:
                role = getattr(msg, 'role', 'assistant')
                content = getattr(msg, 'content', "") or ""
                tool_calls = getattr(msg, 'tool_calls', None)
                
                if role == 'assistant' and tool_calls:
                    tc_list = []
                    for tc in tool_calls:
                        tc_list.append({
                            "id": tc.id, 
                            "type": "function", 
                            "function": {"name": tc.function.name, "arguments": tc.function.arguments}
                        })
                    training_messages.append({"role": "assistant", "content": content, "tool_calls": tc_list})
                else:
                    # 如果是没有调用工具的纯文本回答
                    training_messages.append({"role": role, "content": content})

        final_answer = f"```sql\n{best_pred_sql}\n```" if best_pred_sql else "Failed to generate SQL."
        return final_answer, training_messages, is_invalid_for_training

# ==========================================
# 数据合成单条处理逻辑
# ==========================================
def process_single_item(idx, item, agent: PrivacyProtectionAgent, total_num, db_base_dir, cache_dir):
    case_id = str(item.get("question_id", f"idx_{idx}"))
    db_id = item.get('db_id', 'unknown_db')
    gold_sql = item.get('gold_sql', '')
    
    cache_file = os.path.join(cache_dir, f"{case_id}.json")
    if os.path.exists(cache_file):
        logging.info(f"⏭️ Case [{idx+1}/{total_num}] 缓存存在，跳过。")
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    db_path = f"{db_base_dir}/{db_id}/{db_id}.sqlite"
    
    try:
        result_text, training_messages, is_invalid_for_training = agent.process_mcts(
            item, 
            case_id=case_id, 
            db_path=db_path,
            num_simulations=24, 
            expansion_width=3
        )
        
        pred_sql = find_result(result_text)
        is_correct = 0
        
        # ==========================================
        # [修复点 1] 把执行 SQL 的校验逻辑补回来！
        # ==========================================
        if pred_sql and gold_sql:
            is_correct = execute_sql(pred_sql, gold_sql, db_path, logging, timeout_sec=20.0)
        
        if not is_correct or is_invalid_for_training:
            training_messages = []
            logging.warning(f"🚫 Case [{idx+1}/{total_num}] 舍弃训练数据 (SQL正确:{bool(is_correct)}, 隐私或语法扣分了)")
        else:
            logging.info(f"🌟 Case [{idx+1}/{total_num}] 完美！已保留为高质量训练数据。")
            
        logging.info(f"✅ Case [{idx+1}/{total_num}] 完成. (正确: {'Yes' if is_correct else 'No'})")
        
        # ==========================================
        # [修复点 2] 补全 has_tool_error 变量，防止 res_data 组装时崩溃
        # ==========================================
        has_tool_error = False # 因为你决定容忍工具错误，这里给个默认的 False 即可
        
    except Exception as e:
        logging.error(f"❌ Case [{idx+1}/{total_num}] 失败: {str(e)}")
        result_text, pred_sql, training_messages, is_correct, has_tool_error = f"Error: {str(e)}", "", [], 0, True

    res_data = {
        "idx": idx, "case_id": case_id, "db_id": db_id, "question": item.get('question', ''),
        "gold_sql": gold_sql, "result_text": result_text, "pred_sql": pred_sql,
        "training_messages": training_messages, "is_correct": is_correct, "has_tool_error": has_tool_error
    }
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(res_data, f, ensure_ascii=False, indent=2)
    return res_data

# ==========================================
# 线程池调度核心
# ==========================================
def run_generation(agent, data_path, db_root_path, output_dir, inference_data_name, train_data_dir, max_workers=10):
    data = read_data(data_path)
    total_data = len(data)
    
    reasoning_file_dir = os.path.join(output_dir, inference_data_name, "agent_synthesis")
    cache_dir = os.path.join(reasoning_file_dir, "cache")
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(train_data_dir, exist_ok=True)
    
    pred_sql_path = os.path.join(reasoning_file_dir, 'predict.sql')
    training_data_path = os.path.join(train_data_dir, f"{inference_data_name}_training_data.jsonl") 

    results_list = []
    
    # ⚠️ 警告: MCTS 单次任务开销远大于贪婪搜索，建议将 max_workers 调低以防触发 API 并发限制
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_item, idx, item, agent, total_data, db_root_path, cache_dir): idx for idx, item in enumerate(data)}
        for future in as_completed(futures):
            try: results_list.append(future.result())
            except Exception as e: logging.error(f"线程执行异常: {str(e)}")

    results_list.sort(key=lambda x: x["idx"])

    with open(pred_sql_path, 'w', encoding='utf-8') as f_sql, open(training_data_path, 'w', encoding='utf-8') as f_train:
        for res in results_list:
            f_sql.write((res["pred_sql"] or "") + '\n')
            if res.get("training_messages"):
                f_train.write(json.dumps({"messages": res["training_messages"]}, ensure_ascii=False) + '\n')

    logging.info(f"🎉 数据合成完毕！成功训练样本已存入: {training_data_path}")

if __name__ == '__main__':
    BASE_URL = "http://127.0.0.1:8001/v1"
    MODEL_NAME = 'Qwen3.6-27B'
    API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")
    
    eval_data_base_dir = str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/data')
    output_dir = str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/mcts/output')
    train_data_dir = str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/mcts/train_data')
    db_base_dir = str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1')
    inference_data_name_list = ['bird_train_sc_filtered']

    agent = PrivacyProtectionAgent(base_url=BASE_URL, model_name=MODEL_NAME, api_key=API_KEY)
    
    for inference_data_name in inference_data_name_list:
        data_path = f"{eval_data_base_dir}/{inference_data_name}.jsonl"
        run_generation(agent, data_path, db_base_dir, output_dir, inference_data_name, train_data_dir, max_workers=5) # 建议 MCTS 下调低并发数