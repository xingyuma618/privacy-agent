
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[2]))

import os
import json
import logging
from openai import OpenAI
from typing import Dict, List, Any
import re
import copy
import subprocess
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed # 新增：引入多线程库

# 如果你需要特定的依赖，请取消注释
# from transformers import AutoModelForCausalLM, AutoTokenizer
# import sqlglot
# import sqlite3
import json_repair  

# 导入你已有的模版文件
from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template
from tools import generate_schema_string_from_masked

from transformers import AutoModelForCausalLM, AutoTokenizer
moldel_path = '/mnt/wh_intern/MXY/models/Qwen2.5-Coder-7B-Instruct'
tokenizer = AutoTokenizer.from_pretrained(moldel_path)

# 配置日志格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==========================================
# 工具函数：SQL 清理与提取
# ==========================================
def clean_space(sql):
    sql = re.sub(r'\s*\)', ')', sql)
    #聚合函数括号需要连着
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
    #非聚合函数，括号只删除左括号右边空格
    sql = re.sub(r'\(\s*', '(', sql)
    #删除多余空格
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
    # 1. 根据不同格式读取数据为列表
    if path.endswith('.jsonl'):
        with open(path, "r", encoding=encoding) as f:
            data = [json.loads(line) for line in f if line.strip()]
    else:
        with open(path, "r", encoding=encoding) as f:
            data = json.load(f)
            
    # 2. 根据 question_id 进行升序排序
    sorted_data = sorted(data, key=lambda x: int(x.get('question_id')))
    
    return sorted_data[:100]
        

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
你是一个NL2SQL隐私保护智能体。你需要使用 ReAct (Reasoning and Acting) 范式来完成自然语言查询的隐私脱敏任务。
在这个范式下，你必须交替进行 思考 (Thought) 和 行动 (Action)，并在每次行动后根据收到的 观察 (Observation) 继续下一步，直到得出最终结论。

敏感数据定义：
只要自然语言查询中存在与数据库的表、列、值相关的实体，则均当作敏感数据。

【处理流程与思考链】
请严格遵循以下步骤：
1. 评估结构：分析用户的原Query，评估是否能直接对其隐私实体进行替换。如果直接替换会破坏句法结构，请调用 `query_rewrite` 工具进行改写。
2. 提取实体：使用最终决定使用的 Query（原句或改写后），调用 `entity_extract` 工具提取所有隐私实体。
3. 掩码操作：拿到实体列表后，调用 `apply_typed_masking` 工具进行同频掩码。
4. 评估掩码：评估掩码后的query和schema是否合理。
   - 如果存在语句结构不完整，说明需要调用query_rewrite调整query语句结构再次进行掩码。
   - 如果存在实体漏掩码，说明entity_extract步骤提取的实体不完整。此时你需要通过 `extract_instruction` 参数明确指出哪些实体漏提取了，并【必须】通过 `previous_entities` 参数传入之前已经成功提取的实体列表，再次调用 `entity_extract` 工具。引导工具将新旧实体合并提取，避免丢弃已有成果。拿到新的完整实体列表后，再次调用 `apply_typed_masking`。
5. 生成SQL：将掩码的query与schema传递给 `sql_generate` 生成脱敏SQL。
6. 还原SQL：拿到生成的脱敏SQL后，调用 `sql_unmask` 工具，传入SQL和反向映射字典还原真实SQL。

【输出要求】
在决定调用任何工具前，你必须先在回答中输出你的思考过程！每次回复都应该符合以下逻辑：
Thought: [在这里写下你的思考过程。分析当前的输入、前一步的Observation，以及接下来需要做哪一步操作。]
Action: [通过触发相应的 function call 来执行你的行动]

当工具执行完毕后，系统会将结果作为 Observation 返回给你。

当你执行完所有步骤（即 `sql_unmask` 返回了最终的真实 SQL 后）：
Thought: [表明我已经得到了最终还原的 SQL，任务完成。]
直接输出最终的真实 SQL 作为最终回复，无需再次调用工具。你需要将最终SQL放置在```sql\n\n``` markdown格式中。
"""
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "query_rewrite",
                    "description": "当原query表述不规范时调用此工具。如果是多次调用，可以传入改写说明(rewrite_instruction)以纠正之前的错误。",
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
                    "description": "提取 Schema Linking 实体信息数组。当发现实体漏提取时，可通过 extract_instruction 传入指引，并务必通过 previous_entities 传入已提取好的实体，以进行合并提取。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query_to_extract": {
                                "type": "string",
                                "description": "需要提取实体的查询语句"
                            },
                            "extract_instruction": {
                                "type": "string",
                                "description": "（可选）提取指引或纠错说明。例如：'漏提取了表名XXX和列名YYY，请补充提取'"
                            },
                            "previous_entities": {
                                "type": "array",
                                "description": "（可选）之前已经成功提取出的实体数组对象。必须传入以避免被模型遗忘。",
                                "items": {"type": "object"}
                            }
                        },
                        "required": ["query_to_extract"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "apply_typed_masking",
                    "description": "在底层同时对 Query 和 Structured Schema 执行同频表、列、值掩码替换。",
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
                    "description": "基于掩码后的查询(masked_query)和掩码后的Schema(masked_schema)生成最终的SQL语句。",
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
                    "description": "将掩码后的SQL还原为真实的SQL语句。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "masked_sql": {"type": "string"},
                            "unmask_dict": {"type": "object"}
                        },
                        "required": ["masked_sql", "unmask_dict"]
                    }
                }
            }
        ]

    def _tool_query_rewrite(self, db_schema: str, query_to_rewrite: str, rewrite_instruction: str = "", logger=None) -> str:
        rewrite_client = OpenAI(
            api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),  
            base_url="http://127.0.0.1:2335/v1"  
        )
        model_name = str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-25')
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_rewrite}"
        
        if rewrite_instruction:
            input_str += f"\n- Rewrite Instruction:\n{rewrite_instruction}"
            
        messages = get_query_rewrite_messages(input_str)
        response = rewrite_client.chat.completions.create(model=model_name, messages=messages, temperature=self.temperature)
        ans = response.choices[0].message.content.strip()
        return ans

    def _tool_entity_extract(self, db_schema: str, query_to_extract: str, extract_instruction: str = "", previous_entities: Any = None, logger=None) -> str:
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_extract}"
        
        if previous_entities and isinstance(previous_entities, list) and len(previous_entities) > 0:
            try:
                prev_entities_str = json.dumps(previous_entities, ensure_ascii=False)
                input_str += f"\n- Previously Extracted Entities (Please RETAIN these in your final output):\n{prev_entities_str}"
            except Exception as e:
                if logger: logger(f"Warning: Failed to parse previous_entities: {e}")

        if extract_instruction:
            input_str += f"\n- Extract Instruction (Please ADD these missing entities):\n{extract_instruction}"
            
        messages = get_entity_extract_template(input_str)
        entity_extract_client = OpenAI(
            api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),  
            base_url="http://127.0.0.1:2336/v1"  
        )
        model_name = str(_RELEASE_ROOT / 'LlamaFactory/saves/entity_extract_v1/checkpoint-25')
        response = entity_extract_client.chat.completions.create(model=model_name, messages=messages, temperature=self.temperature)

        response_text = response.choices[0].message.content.strip()
            
        match = re.search(r'\[\s*\{.*?\}\s*\]', response_text, re.DOTALL)
        return match.group(0) if match else "[]"
    
    def _tool_sql_generate(self, masked_query, masked_schema_str, logger=None):
        input_str = f"- Database Schema:\n{masked_schema_str}\n- User's Question:\n{masked_query}"
        messages = get_sql_generate_template(input_str)
        response = self.client.chat.completions.create(model=self.model_name, messages=messages, temperature=self.temperature)
        response_text = response.choices[0].message.content.strip()
        
        return response_text

    def _tool_sql_unmask(self, masked_sql: str, unmask_dict: Any) -> str:
        # 1. 处理传入的 unmask_dict 可能为 JSON 字符串的情况
        if isinstance(unmask_dict, str):
            try:
                unmask_dict = json_repair.loads(unmask_dict)
            except Exception:
                pass
                
        # 2. 基础校验：如果不符合格式，直接返回原 SQL
        if not masked_sql or not isinstance(masked_sql, str) or not isinstance(unmask_dict, dict):
            return masked_sql

        # 3. 将嵌套字典展平为列表，方便统一遍历替换
        mask_info_list = []
        for category in ["tables", "columns", "values"]:
            if category in unmask_dict:
                for mask, orig in unmask_dict[category].items():
                    mask_info_list.append({
                        "mask": mask,
                        "original": str(orig),
                        "category": category
                    })

        # 4. 关键：按 mask 长度降序排序！防止短掩码破坏长掩码 (例如 val_1 错误匹配到 val_10)
        mask_info_list.sort(key=lambda x: len(x["mask"]), reverse=True)
        unmasked_text = masked_sql

        # 5. 遍历替换
        for info in mask_info_list:
            mask = info["mask"]
            original_value = info["original"]
            category = info["category"]
            
            # 使用 \b 确保只匹配完整的单词
            pattern = re.compile(r'\b' + re.escape(mask) + r'\b', re.IGNORECASE)
            
            def replace_match(match):
                text = match.string
                start = match.start()
                end = match.end()
                
                # 获取匹配词左右的字符，用于判断是否已经被引号包裹，或者是否有表别名前缀
                left_char = text[start - 1] if start > 0 else ""
                right_char = text[end] if end < len(text) else ""
                
                val = original_value
                
                # 【核心修复逻辑】：智能处理带前缀的列名还原
                if category == "columns" and "." in val:
                    tbl_part, col_part = val.split(".", 1)
                    if left_char == ".":
                        # 情况 A: SQL 中已经存在表别名 (如 t2.col_4)
                        # 左侧是 '.'，说明前面有别名。我们丢弃映射中的表名，只保留列名。
                        val = col_part
                    else:
                        # 情况 B: SQL 中没有表别名 (如独立出现的 col_4)
                        # 我们把表名和列名分别用反引号包裹，生成标准的 `table`.`column`
                        res = f"`{tbl_part}`.`{col_part}`"
                        
                        # 如果外层没有被其他引号包裹，直接返回包裹好的安全结果
                        if left_char not in ("`", "'", '"') and right_char not in ("`", "'", '"'):
                            return res
                        return f"{tbl_part}.{col_part}"

                # 常规逻辑：针对表名，或者已经剥离了表前缀的单独列名，进行安全的反引号包裹
                if category in ["tables", "columns"]:
                    # 避免重复添加引号 (如把 'val' 变成 `'val'`)
                    if left_char not in ("`", "'", '"') and right_char not in ("`", "'", '"'):
                        return f"`{val}`"
                        
                # 针对 values 或者不需要加反引号的情况，直接返回
                return val

            # 执行替换
            unmasked_text = pattern.sub(replace_match, unmasked_text)

        return unmasked_text

    def _tool_apply_typed_masking(self, query, entities, structured_db_schema):
        table_map = {}
        col_map = {}
        val_map = {}
        
        t_idx = 1
        c_idx = 1
        v_idx = 1
        
        # 1. 遍历 Schema，仅构建 表名(Table) 和 列名(Column) 的映射
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
                    
        # 【重要优化】删除了对 examples 的遍历。

        # 2. 遍历 Entities，仅从这里获取并构建 值(Value) 的映射
        for ent in entities:
            if ent.get("entity_type") == "cell_value" and ent.get("cell_value") is not None:
                val_str = str(ent["cell_value"])
                if not val_str.strip(): # 依然保留防御：过滤纯空格
                    continue
                    
                if val_str not in val_map:
                    val_map[val_str] = f"val_{v_idx}"
                    v_idx += 1

        # 辅助函数：用于全局扫尾的字符串替换
        def replace_in_string(text):
            if not isinstance(text, str) or not text: 
                return text
            replace_dict = {orig: mask for orig, mask in table_map.items()}
            replace_dict.update({orig_c: mask for (orig_t, orig_c), mask in col_map.items()})
            replace_dict.update({orig: mask for orig, mask in val_map.items()})
            
            # 长度倒序，防止短词破坏长词（如防止 id 破坏 student_id）
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

        # 3. 优先对 Query 中显式指出的实体进行精准替换
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
                
        # 全局最后一次扫尾替换（处理 entities 里没有圈出来的隐藏列名或表名）
        masked_query = replace_in_string(masked_query)

        # 4. 脱敏 Schema 的元数据 (由于你最终是要转为 DDL, 这里只替换字典内的键和关键值)
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
                        # 【重要优化】如果不需要例子，这里甚至可以直接忽略 examples 键
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
                    if k not in ["columns", "examples"]: # 同样忽略外层的 examples（如果有）
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

        # 5. 构建还原字典
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
    def process(self, item: Dict[str, Any], case_id: str = "default") -> str:
        
        log_dir = "case_logs"
        os.makedirs(log_dir, exist_ok=True) # 线程安全
        log_file_path = os.path.join(log_dir, f"case_{case_id}.txt")
        
        if os.path.exists(log_file_path):
            open(log_file_path, 'w', encoding='utf-8').close()

        def log_to_file(text: str):
            with open(log_file_path, "a", encoding="utf-8") as f:
                f.write(text + "\n")

        original_question = item['question']
        db_schema = item.get('db_schema', '') 
        evidence = item.get("evidence", '')
        structured_db_schema = item.get('structured_db_schema', {}) 
        
        log_to_file(f"{'='*30} [CASE START: {case_id}] {'='*30}")
        log_to_file(f"[DB SCHEMA]:\n{db_schema}\n")
        log_to_file(f"[USER QUESTION]:\n{original_question}\n")
        log_to_file(f"[EVIDENCE]:\n{evidence}\n")
        log_to_file(f"{'-'*80}\n")

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": f"Database Schema Text:\n{db_schema}\n\nUser Question:\n{original_question} {evidence}\n\n任务开始，请给出你的分析（Thought）并调用工具（Action）。"}
        ]
        
        logging.info("="*50)
        logging.info(f"🚀 开始处理新 Query: {original_question} (Case ID: {case_id})")
        masked_schema = ''
        masked_sql = ''
        unmask_dict = ''
        masked_question = ''
        for iteration in range(20):
            # 防止过多日志冲刷控制台，将底层循环日志精简或视情况保留
            # logging.info(f"🔄 [Case: {case_id}] [第 {iteration + 1} 轮推理思考中...]")
            log_to_file(f"\n{'='*20} [Round {iteration + 1}] {'='*20}")
            
            response = self.client.chat.completions.create(
                model=self.model_name, messages=messages, tools=self.tools, tool_choice="auto", temperature=self.temperature
            )
            message = response.choices[0].message
            messages.append(message) 
            
            thought_text = message.content.strip() if message.content else "（直接输出，无文本思考）"
            log_to_file(f"🧠 [Thought]:\n{thought_text}\n")
            
            parsed_tool_calls = []
            if message.content:
                matches = re.finditer(r'<tool_call>\s*(.*?)\s*</tool_call>', message.content, re.DOTALL)
                for match_idx, match in enumerate(matches):
                    try:
                        tool_data = json_repair.loads(match.group(1))
                        parsed_tool_calls.append({"name": tool_data.get("name"), "arguments": tool_data.get("arguments", {}), "id": f"call_patch_{iteration}_{match_idx}"})
                        message.content = message.content.replace(match.group(0), "")
                    except json.JSONDecodeError:
                        pass

            if message.tool_calls or parsed_tool_calls:
                calls_to_process = [{"name": tc.function.name, "arguments": json_repair.loads(tc.function.arguments) if isinstance(tc.function.arguments, str) else tc.function.arguments, "id": tc.id} for tc in message.tool_calls] if message.tool_calls else parsed_tool_calls

                for tc_data in calls_to_process:
                    func_name, args, tool_call_id = tc_data["name"], tc_data["arguments"], tc_data["id"]
                    log_args_str = json.dumps(args, ensure_ascii=False, indent=2)
                    
                    log_to_file(f"🛠️ [Action - Tool Call]: {func_name}")
                    log_to_file(f"   [Action - Arguments]: {log_args_str}\n")
                    
                    try:
                        if func_name == "query_rewrite":
                            result = self._tool_query_rewrite(db_schema, args.get('query_to_rewrite', ''), args.get('rewrite_instruction', ''), logger=log_to_file)
                        elif func_name == "entity_extract":
                            result = self._tool_entity_extract(
                                db_schema, 
                                args.get('query_to_extract', ''), 
                                args.get('extract_instruction', ''), 
                                args.get('previous_entities', []),
                                logger=log_to_file
                            )
                        elif func_name == "apply_typed_masking":
                            result = self._tool_apply_typed_masking(args.get('query', ''), args.get('entities', []), structured_db_schema)
                        elif func_name == "sql_generate":
                            masked_schema = args.get('masked_schema_str', '')
                            masked_question = args.get('masked_query', '')
                            result = self._tool_sql_generate(args.get('masked_query', ''), args.get('masked_schema_str', ''), logger=log_to_file)
                        elif func_name == "sql_unmask":
                            masked_sql = args.get('masked_sql', '')
                            unmask_dict = args.get('unmask_dict', {})
                            result = self._tool_sql_unmask(args.get('masked_sql', ''), args.get('unmask_dict', {}))
                        else:
                            result = f"Error: 未知工具 {func_name}"
                    except Exception as e:
                        result = f"Error executing {func_name}: {str(e)}"
                    
                    log_result_str = str(result)
                    log_to_file(f"👀 [Observation - Result]:\n{log_result_str}\n")
                    
                    messages.append({"role": "tool", "tool_call_id": tool_call_id, "name": func_name, "content": str(result)})

            else:
                logging.info(f"🏁 任务结束 [Case: {case_id}]")
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
                
                return message.content
        
        logging.error(f"❌ Agent 思考超时 (Case ID: {case_id})")
        log_to_file(f"\n{'='*30} [CASE TIMEOUT] {'='*30}")
        log_to_file("Agent exceeded maximum iterations.")
        return '{"error": "Agent timeout"}'


# ==========================================
# 评估模块核心逻辑
# ==========================================
def eval_exec(predicted_sql_path, db_root_path, data_mode, ground_truth_path, num_cpus, mode_gt, mode_predict, diff_json_path, meta_time_out, output_dir, inference_data_name):
    if 'spider' in inference_data_name:
        predicted_sql_path = predicted_sql_path.replace('json', 'sql')
        # Spider 路径替换逻辑
        db_root_path = '/root/data/nl2sql/spider/database'
        if 'syn' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_syn_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        elif 'real' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/spider_real_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        elif 'test' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/test_tables.json'
        elif 'dev' in inference_data_name:
            ground_truth_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/dev_gold.sql'
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        else:
            table_path = '/mnt/wh_intern/MXY/paper/MAC-SQL/data/spider/tables.json'
        
        eval_cmd = f'''nohup python spider_eval/evaluation.py \
        --gold {ground_truth_path} \
        --pred {predicted_sql_path} \
        --etype all \
        --db {db_root_path} \
        --table {table_path} > {output_dir}/eval.log 2>&1 &
        ''' 
    else:
        # BIRD 逻辑
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
    logging.info(f"🚀 开始执行评估脚本...\n{eval_cmd}")
    subprocess.run(eval_cmd, shell=True, check=True)

def run_evaluation(agent: PrivacyProtectionAgent, data_path: str, db_root_path: str, output_dir: str, inference_data_name: str, diff_json_path: str, data_mode: str, ground_truth_path: str, max_workers: int = 8):
    """
    使用 Agent 对数据集执行多线程推理，提取 SQL 并跑测试
    """
    logging.info(f"加载数据集: {data_path}")
    data = read_data(data_path)
    
    # 构建输出路径
    reasoning_file_dir = os.path.join(output_dir, inference_data_name, "agent_eval")
    os.makedirs(reasoning_file_dir, exist_ok=True)
    
    pred_sql_path = os.path.join(reasoning_file_dir, 'predict.sql')
    json_result_path = os.path.join(reasoning_file_dir, 'predict.json')
    all_res_path = os.path.join(reasoning_file_dir, 'all_res.json')

    # 用于保存有序结果的数组，避免多线程写入文件乱序
    results = [None] * len(data)
    
    def process_single_item(idx, item):
        """线程工作函数"""
        case_id = str(item.get("question_id", f"idx_{idx}"))
        db_id = item.get('db_id', 'unknown_db')
        gold_sql = item.get('gold_sql', '')
        
        try:
            # 执行 Agent 推理
            result_text = agent.process(item, case_id=case_id)
            pred_sql = find_result(result_text)
        except Exception as e:
            logging.error(f"❌ [Case {case_id}] Agent 处理异常: {e}")
            logging.error(traceback.format_exc())
            result_text = f"Error: {e}"
            pred_sql = "SELECT 1" # 回退的安全 SQL，避免下游评估全线崩溃
            
        return {
            'idx': idx,
            'db_id': db_id,
            'input': item.get('question', ''),
            'output': result_text,
            'pred_sql': pred_sql,
            'gold_sql': gold_sql
        }

    logging.info(f"⚡ 启动多线程并发处理，最大线程数: {max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务，并将 future 映射回原始索引 idx
        future_to_idx = {executor.submit(process_single_item, idx, item): idx for idx, item in enumerate(data)}
        
        completed_count = 0
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                res = future.result()
                results[idx] = res # 严格塞入对应位置，保证最终文件的顺序不出错
                completed_count += 1
                logging.info(f"✅ 完成进度: {completed_count}/{len(data)} (Question ID: {data[idx].get('question_id', 'unknown')})")
            except Exception as exc:
                logging.error(f"❌ [线程级异常] 用例 Index {idx} 崩溃: {exc}")
                # 放一个占位结果防止后续打包报错
                results[idx] = {
                    'idx': idx, 'db_id': data[idx].get('db_id', ''), 
                    'input': data[idx].get('question', ''), 'output': f"Thread Error: {exc}", 
                    'pred_sql': "", 'gold_sql': data[idx].get('gold_sql', '')
                }

    # 汇总写入文件，顺序与 data 原本严格保持一致
    all_res = []
    bird_eval_dic = {}
    
    logging.info(f"💾 开始将结果汇总并写入文件...")
    with open(pred_sql_path, 'w', encoding='utf-8') as f_sql:
        for item_res in results:
            if not item_res: continue
            
            idx = item_res['idx']
            pred_sql = item_res['pred_sql']
            db_id = item_res['db_id']
            
            f_sql.write(pred_sql + '\n')
            bird_eval_dic[idx] = pred_sql + '\t----- bird -----\t' + db_id
            all_res.append(item_res)

    # 保存 JSON
    with open(json_result_path, 'w', encoding='utf-8') as f_json:
        json.dump(bird_eval_dic, f_json, indent=4)
        
    with open(all_res_path, 'w', encoding='utf-8') as f_all:
        json.dump(all_res, f_all, indent=4)

    # 执行外挂脚本评估
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
    eval_exec(**eval_kwargs)
    logging.info(f"🏁 评估完成！结果储存在: {reasoning_file_dir}")


if __name__ == '__main__':
    # 初始化
    # BASE_URL = "http://127.0.0.1:8001/v1"
    # MODEL_NAME = 'Qwen3.5-122B-A10B'
    # API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

    
    BASE_URL = "https://api.deepseek.com/v1"  
    MODEL_NAME = 'deepseek-chat'
    API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

    eval_data_base_dir = str(_RELEASE_ROOT / 'privacy-agent/agent/data')
    output_dir = str(_RELEASE_ROOT / 'privacy-agent/agent/output')
    
    # 评估数据集设置
    inference_data_name_list = ['bird_dev_sc']
    
    # BIRD 设置
    db_base_dir = str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1')
    diff_json_path = '/mnt/wh_intern/MXY/paper/rl/ours/open-r1/data/bird/diff.json'
    data_mode = 'dev'
    ground_truth_path = '/mnt/wh_intern/MXY/paper/rl/ours/CHESS/data/dev/dev.sql'

    # 设定并发线程数
    MAX_WORKERS = 10 

    # ================= 初始化 Agent =================
    agent = PrivacyProtectionAgent(base_url=BASE_URL, model_name=MODEL_NAME, api_key=API_KEY)
    
    # ================= 遍历并执行评估 =================
    for inference_data_name in inference_data_name_list:
        data_path = f"{eval_data_base_dir}/{inference_data_name}.jsonl"
        
        logging.info(f"====================================")
        logging.info(f"开始评估数据集: {inference_data_name}")
        
        # 触发评估循环
        run_evaluation(
            agent=agent,
            data_path=data_path,
            db_root_path=db_base_dir,
            output_dir=output_dir,
            inference_data_name=inference_data_name,
            diff_json_path=diff_json_path,
            data_mode=data_mode,
            ground_truth_path=ground_truth_path,
            max_workers=MAX_WORKERS # 传入多线程数量参数
        )