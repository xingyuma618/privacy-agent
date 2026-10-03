
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
from transformers import AutoModelForCausalLM, AutoTokenizer

moldel_path = '/mnt/wh_intern/MXY/models/Qwen2.5-Coder-7B-Instruct'
tokenizer = AutoTokenizer.from_pretrained(moldel_path)

# 导入你已有的模版文件
from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template

from tools import generate_schema_string_from_masked

# 配置日志格式，让输出的层级更清晰
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
你是一个NL2SQL隐私保护智能体。你需要自主调度工具来完成自然语言查询的隐私脱敏任务，请注意，每个步骤你只能使用一个工具。
敏感数据定义：
只要自然语言查询中存在与数据库的表、列、值相关的实体，则均当作敏感数据。

请遵循以下思考链：
1. 由于替换query中的实体可能会破坏句子结构，所以你需要先分析用户的原Query，评估其结构，是否能够直接对其隐身实体体进行替换，如果存在直接替换隐私实体会破坏句法结构的情况，请调用 `query_rewrite` 工具进行改写。
例如：
How much does Alice earn per month?
Alice对应的是值， earn per month 对应的是salary列，
如果直接替换，则会出现。
How much dose [value_1] [column_1]
显然使得这句话变得难以理解，因此你需要调用query_rewrite工具进行句式调整。
例如调整为：
"What is the salary of the employee matching the record Alice?"
则此时替换后为，不再影响理解。
"What is the [column] of the [table] matching the record [value]?"

2. 使用最终决定使用的 Query（原句或改写后的句子），调用 `entity_extract` 工具提取所有的隐私实体。
3. 拿到实体列表后，调用 `apply_typed_masking` 工具，将query进行掩码操作。
4. 评估掩码后的query和schema是否合理
- 如果存在语句结构不完整或与原始问题差异较大，导致看不出query要查询什么，说明需要调用query_rewrite调整query语句结构再次进行掩码。
- 如果存在实体漏掩码，说明entity_extract步骤提取的实体不完整，你需要将你观察到的漏掉的实体直接补充到entities中，并且再次调用apply_typed_masking
5. 将掩码的query与schema传递给 `sql_generate` 工具进行SQL语句生成。
6. 拿到生成的掩码SQL后，调用 `sql_unmask` 工具，传入生成的脱敏SQL和步骤3返回的反向映射字典(unmask_dict)，将其还原为真实的SQL语句，作为最终结果输出。
"""
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "query_rewrite",
                    "description": "当原query表述不规范时调用此工具。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query_to_rewrite": {"type": "string"}
                        },
                        "required": ["query_to_rewrite"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "entity_extract",
                    "description": "提取 Schema Linking 实体信息数组。",
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
                    "description": "基于掩码后的查询(masked_query)和掩码后的Schema(masked_schema)生成最终的SQL语句。在apply_typed_masking返回结果后使用。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "masked_query": {
                                "type": "string",
                                "description": "由 apply_typed_masking 工具返回的脱敏后的用户问题"
                            },
                            "masked_schema_str": {
                                "type": "string",
                                "description": "由 apply_typed_masking 工具返回的脱敏后的数据库Schema字符串"
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
                    "description": "将掩码后的SQL还原为真实的SQL语句。在sql_generate返回结果后使用。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "masked_sql": {
                                "type": "string",
                                "description": "由 sql_generate 工具生成的脱敏SQL"
                            },
                            "unmask_dict": {
                                "type": "object",
                                "description": "由 apply_typed_masking 工具返回的反向映射字典"
                            }
                        },
                        "required": ["masked_sql", "unmask_dict"]
                    }
                }
            }
        ]

    def _tool_query_rewrite(self, db_schema: str, query_to_rewrite: str, logger=None) -> str:
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_rewrite}"
        messages = get_query_rewrite_messages(input_str)
            
        response = self.client.chat.completions.create(model=self.model_name, messages=messages, temperature=self.temperature)
        ans = response.choices[0].message.content.strip()
        return ans

    def _tool_entity_extract(self, db_schema: str, query_to_extract: str, logger=None) -> str:
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_extract}"
        messages = get_entity_extract_template(input_str)
            
        response = self.client.chat.completions.create(model=self.model_name, messages=messages, temperature=self.temperature)
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
        if isinstance(unmask_dict, str):
            try:
                unmask_dict = json.loads(unmask_dict)
            except Exception:
                pass
                
        if not masked_sql or not isinstance(masked_sql, str) or not isinstance(unmask_dict, dict):
            return masked_sql

        flat_map = {}
        for category in ["tables", "columns", "values"]:
            if category in unmask_dict:
                flat_map.update(unmask_dict[category])

        sorted_masks = sorted(flat_map.keys(), key=len, reverse=True)

        unmasked_text = masked_sql
        for mask in sorted_masks:
            original_value = str(flat_map[mask])
            pattern = re.compile(r'\b' + re.escape(mask) + r'\b', re.IGNORECASE)
            unmasked_text = pattern.sub(lambda _: original_value, unmasked_text)

        return unmasked_text

    def _tool_apply_typed_masking(self, query, entities, structured_db_schema):
        table_map = {}
        col_map = {}
        val_map = {}
        
        t_idx = 1
        c_idx = 1
        v_idx = 1
        
        for table_name, table_info in structured_db_schema.items():
            if table_name not in table_map:
                table_map[table_name] = f"table_{t_idx}"
                t_idx += 1
                
            if "columns" in table_info:
                cols = table_info["columns"]
            else:
                cols = [{"column_name": k, **v} for k, v in table_info.items()]
                
            for col_info in cols:
                col_name = col_info["column_name"]
                if (table_name, col_name) not in col_map:
                    col_map[(table_name, col_name)] = f"col_{c_idx}"
                    c_idx += 1
                    
                for example in col_info.get("examples", []):
                    val_list = example if isinstance(example, list) else [example]
                    for val in val_list:
                        val_str = str(val)
                        if val_str not in val_map:
                            val_map[val_str] = f"val_{v_idx}"
                            v_idx += 1
                            
        for ent in entities:
            if ent.get("entity_type") == "cell_value" and ent.get("cell_value"):
                val_str = str(ent["cell_value"])
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
                if re.match(r'^\w+$', k_str):
                    res = re.sub(r'\b' + re.escape(k_str) + r'\b', replace_dict[k], res, flags=re.IGNORECASE)
                else:
                    res = res.replace(k_str, replace_dict[k])
            return res

        masked_query = query
        sorted_entities = sorted(entities, key=lambda x: len(str(x.get("entity", ""))), reverse=True)
        
        for ent in sorted_entities:
            ent_str = str(ent["entity"])
            mask = None
            
            if ent["entity_type"] == "table":
                mask = table_map.get(ent["table"])
            elif ent["entity_type"] == "column":
                mask = col_map.get((ent["table"], ent["column"]))
            elif ent["entity_type"] == "cell_value":
                mask = val_map.get(str(ent["cell_value"]))
                
            if mask:
                pattern = re.compile(re.escape(ent_str), re.IGNORECASE)
                masked_query = pattern.sub(mask, masked_query)
                
        masked_query = replace_in_string(masked_query)

        masked_schema = {}
        for table_name, table_info in structured_db_schema.items():
            m_table_name = table_map[table_name]
            m_table_info = {}
            
            if "columns" in table_info:
                m_table_info["columns"] = []
                for col_info in table_info["columns"]:
                    m_col_info = {}
                    for attr_key, attr_val in col_info.items():
                        if isinstance(attr_val, str):
                            m_col_info[attr_key] = replace_in_string(attr_val)
                        else:
                            m_col_info[attr_key] = attr_val
                    m_col_info["column_name"] = col_map.get((table_name, col_info["column_name"]), col_info["column_name"])
                    m_table_info["columns"].append(m_col_info)
                    
                for k, v in table_info.items():
                    if k != "columns":
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
                        if isinstance(attr_val, str):
                            m_col_attrs[attr_key] = replace_in_string(attr_val)
                        else:
                            m_col_attrs[attr_key] = attr_val
                    m_table_info[m_col_name] = m_col_attrs
                    
            masked_schema[m_table_name] = m_table_info

        unmask_dict = {
            "tables": {mask: orig for orig, mask in table_map.items()},
            "columns": {mask: orig_c for (orig_t, orig_c), mask in col_map.items()},
            "values": {mask: orig for orig, mask in val_map.items()}
        }
        masked_schema_str = generate_schema_string_from_masked(masked_schema)
        
        return json.dumps({
            "masked_query": masked_query, 
            "masked_schema_str": masked_schema_str, 
            "unmask_dict": unmask_dict
        }, ensure_ascii=False)

    # ================= Agent 控制流 =================
    def process(self, item: Dict[str, Any], case_id: str = "default") -> str:
        
        log_dir = "case_logs"
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
        structured_db_schema = item.get('structured_db_schema', {}) 
        
        log_to_file(f"{'='*30} [CASE START: {case_id}] {'='*30}")
        log_to_file(f"[USER QUESTION]:\n{original_question}\n")
        log_to_file(f"[EVIDENCE]:\n{evidence}\n")
        log_to_file(f"{'-'*80}\n")

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": f"Database Schema Text:\n{db_schema}\n\nUser Question:\n{original_question} {evidence}\n\n请开始处理。"}
        ]
        
        logging.info("="*50)
        logging.info(f"🚀 开始处理新 Query: {original_question} (Case ID: {case_id})")
        
        for iteration in range(20):
            logging.info(f"🔄 [第 {iteration + 1} 轮推理思考中...]")
            
            # ==================【本次主要修改区域 开始】==================
            # 1. 打印本轮大模型的 INPUT (使用 tokenizer.apply_chat_template)
            log_to_file(f"\n{'='*20} [Round {iteration + 1} INPUT] {'='*20}")
            
            dict_messages = []
            for msg in messages:
                if isinstance(msg, dict):
                    dict_messages.append(msg)
                else:
                    # 使用 model_dump 转化为字典，过滤空值避免模板报错
                    dict_messages.append({k: v for k, v in msg.model_dump().items() if v is not None})
            
            try:
                # 使用 Tokenizer 还原真实的底层输入字符串
                input_str = tokenizer.apply_chat_template(
                    dict_messages,
                    tokenize=False,
                    add_generation_prompt=True
                )
                log_to_file(input_str)
                # print(input_str)  # 如果你还需要在控制台同步打印，可以取消这行注释
            except Exception as e:
                log_to_file(f"[Apply Chat Template Error]: {e}\n[Fallback Messages]: {json.dumps(dict_messages, ensure_ascii=False)}")

            log_to_file(f"{'-'*55}\n")
            
            # --- 调用大模型 ---
            response = self.client.chat.completions.create(
                model=self.model_name, messages=messages, tools=self.tools, tool_choice="auto", temperature=self.temperature
            )
            # ==================【本次主要修改区域 结束】==================
            
            message = response.choices[0].message
            messages.append(message) 
            print('=====11111111=====',messages)
            # ---------------------------------------------------------
            # 2. 打印本轮大模型的 OUTPUT (思考过程与提取的 Tool)
            # ---------------------------------------------------------
            log_to_file(f"{'='*20} [Round {iteration + 1} OUTPUT] {'='*20}")
            if message.content:
                log_to_file(f"[Agent Thought]:\n{message.content.strip()}\n")
            else:
                log_to_file(f"[Agent Thought]: (无文本输出，直接调用工具)\n")
            parsed_tool_calls = []
            if message.content:
                matches = re.finditer(r'<tool_call>\s*(.*?)\s*</tool_call>', message.content, re.DOTALL)
                for match_idx, match in enumerate(matches):
                    try:
                        tool_data = json.loads(match.group(1))
                        parsed_tool_calls.append({"name": tool_data.get("name"), "arguments": tool_data.get("arguments", {}), "id": f"call_patch_{iteration}_{match_idx}"})
                        message.content = message.content.replace(match.group(0), "")
                    except json.JSONDecodeError:
                        pass
            
            # ---------------------------------------------------------
            # 3. 执行工具并打印结果
            # ---------------------------------------------------------
            if message.tool_calls or parsed_tool_calls:
                calls_to_process = [{"name": tc.function.name, "arguments": json.loads(tc.function.arguments) if isinstance(tc.function.arguments, str) else tc.function.arguments, "id": tc.id} for tc in message.tool_calls] if message.tool_calls else parsed_tool_calls

                for tc_data in calls_to_process:
                    func_name, args, tool_call_id = tc_data["name"], tc_data["arguments"], tc_data["id"]
                    
                    log_args_str = json.dumps(args, ensure_ascii=False, indent=2)
                    logging.info(f"👉 决定调用工具: [{func_name}]")
                    
                    log_to_file(f"[Action - Tool Call]: {func_name}")
                    log_to_file(f"[Action - Arguments]:\n{log_args_str}\n")
                    
                    try:
                        if func_name == "query_rewrite":
                            result = self._tool_query_rewrite(db_schema, args.get('query_to_rewrite', ''), logger=log_to_file)
                        elif func_name == "entity_extract":
                            result = self._tool_entity_extract(db_schema, args.get('query_to_extract', ''), logger=log_to_file)
                        elif func_name == "apply_typed_masking":
                            result = self._tool_apply_typed_masking(
                                args.get('query', ''), 
                                args.get('entities', []), 
                                structured_db_schema 
                            )
                        elif func_name == "sql_generate":
                            result = self._tool_sql_generate(
                                args.get('masked_query', ''),
                                args.get('masked_schema_str', ''),
                                logger=log_to_file
                            )
                        elif func_name == "sql_unmask":
                            result = self._tool_sql_unmask(
                                args.get('masked_sql', ''),
                                args.get('unmask_dict', {})
                            )
                        else:
                            result = f"Error: 未知工具 {func_name}"
                    except Exception as e:
                        result = f"Error executing {func_name}: {str(e)}"
                    
                    log_result_str = str(result)
                    logging.info(f"✅ 工具 [{func_name}] 执行完毕")
                    
                    log_to_file(f"[Action - Result]:\n{log_result_str}\n")
                    
                    messages.append({"role": "tool", "tool_call_id": tool_call_id, "name": func_name, "content": str(result)})
                    print('====2222222======',messages)

            else:
                # ---------------------------------------------------------
                # 4. 任务结束
                # ---------------------------------------------------------
                logging.info(f"🏁 任务结束，输出最终结果。")
                log_to_file(f"\n{'='*30} [CASE END] {'='*30}")
                log_to_file(f"[Final Answer]:\n{message.content}\n")
                
                return message.content
        
        logging.error("❌ Agent 思考超时（超过最大迭代次数）")
        log_to_file(f"\n{'='*30} [CASE TIMEOUT] {'='*30}")
        log_to_file("Agent exceeded maximum iterations.")
        return '{"error": "Agent timeout"}'


def read_jsonl_to_list(path, encoding="utf-8"):
    with open(path, "r", encoding=encoding) as f:
        return [json.loads(line) for line in f if line.strip()]
    
if __name__ == '__main__':
    # 初始化
    BASE_URL = "https://api.deepseek.com/v1"  
    MODEL_NAME = 'deepseek-chat'
    API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

    agent = PrivacyProtectionAgent(base_url=BASE_URL, model_name=MODEL_NAME, api_key=API_KEY)
    
    # 加载测试数据
    data_path = str(_RELEASE_ROOT / 'privacy-agent/agent/data/bird_dev_sc.jsonl')
    data = read_jsonl_to_list(data_path)

    # 测试
    for idx, item in enumerate(data[1:3]):
        # 获取唯一标识（优先取数据集原有的 question_id，如果不存在则使用循环下标）
        case_id = str(item.get("question_id", f"idx_{idx}"))
        
        final_result = agent.process(item, case_id=case_id)
        print(f"\n🟢 Agent Case {case_id} 最终输出:")
        print(final_result)
        print(f"📄 查看详情请打开: case_logs/case_{case_id}.txt\n")

    
# if __name__ == '__main__':
#     # 初始化
#     # BASE_URL = "https://api.moonshot.cn/v1"
#     # MODEL_NAME = 'kimi-k2-0905-preview'
#     # API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

#     BASE_URL = "https://api.deepseek.com/v1"  
#     MODEL_NAME = 'deepseek-chat'
#     API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

#     # BASE_URL = "http://127.0.0.1:2333/v1"
#     # MODEL_NAME = '/root/models/Qwen3-235B-A22B-Instruct'
#     # API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")
    
#     agent = PrivacyProtectionAgent(base_url=BASE_URL, model_name=MODEL_NAME, api_key=API_KEY)
    
#     # 加载测试数据
#     data_path = "/mnt/wh_intern/MXY/paper/privacy-agent/agent/data/bird_dev_sc.jsonl"
#     data = read_jsonl_to_list(data_path)

#     # 测试
#     for idx, item in enumerate(data[1:5]):
#         # 获取唯一标识（优先取数据集原有的 question_id，如果不存在则使用循环下标）
#         case_id = str(item.get("question_id", f"idx_{idx}"))
        
#         final_result = agent.process(item, case_id=case_id)
#         print(f"\n🟢 Agent Case {case_id} 最终输出:")
#         print(final_result)
#         print(f"📄 查看详情请打开: case_logs/case_{case_id}.txt\n")


'''
我希望实现一个这样的python函数，传入参数是
query：用户的自然语言查询
query示例：
Which school in Contra Costa has the highest number of test takers?

structured_db_schema，存储的数据库schema信息
structured_db_schema示例：
"structured_db_schema": {"users": {"columns": [{"column_name": "DisplayName", "column_definition": "DisplayName TEXT null", "data_type": "TEXT", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [], "examples": ["user", "name"], "column_description": "", "value_description": ""}, {"column_name": "CreationDate", "column_definition": "CreationDate DATETIME null", "data_type": "DATETIME", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [], "examples": [["2010-07-19 06:55:26.0"], ["2010-07-19 14:01:36.0"], ["2010-07-19 15:34:50.0"]], "column_description": "", "value_description": ""}, {"column_name": "Id", "column_definition": "Id INTEGER not null UNIQUE", "data_type": "", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [], "examples": [], "column_description": "", "value_description": ""}], "table_primary_keys": ["primary key (Id)"], "table_foreign_keys": [], "unique_constraints": []}}}
{"frpm": {"columns": [{"column_name": "CDSCode", "column_definition": "CDSCode TEXT not null", "data_type": "TEXT", "is_primary_key": true, "foreign_keys_info": [["schools", "CDSCode"]], "referenced_by": [], "examples": [["01100170109835"], ["01100170112607"], ["01100170118489"]], "column_description": "", "value_description": ""}, {"column_name": "Enrollment (K-12)", "column_definition": "`Enrollment (K-12)` REAL null", "data_type": "REAL", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [], "examples": [[1087.0], [395.0], [244.0]], "column_description": "Enrollment (K-12)", "value_description": "K-12: 1st grade - 12nd grade"}, {"column_name": "Enrollment (Ages 5-17)", "column_definition": "`Enrollment (Ages 5-17)` REAL null", "data_type": "REAL", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [], "examples": [[1070.0], [376.0], [230.0]], "column_description": "Enrollment (Ages 5-17)", "value_description": ""}], "table_primary_keys": ["PRIMARY KEY (CDSCode)"], "table_foreign_keys": [], "unique_constraints": []}}}
{"district": {"columns": [{"column_name": "A15", "column_definition": "A15 INTEGER null", "data_type": "INTEGER", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [], "examples": [[85677], [2159], [2824]], "column_description": "no. of committed crimes 1995", "value_description": ""}], "table_primary_keys": ["district_id INTEGER default 0 not null primary key"], "table_foreign_keys": [], "unique_constraints": []}, "account": {"columns": [{"column_name": "date", "column_definition": "date DATE not null", "data_type": "DATE", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [], "examples": [["1995-03-24"], ["1993-02-26"], ["1997-07-07"]], "column_description": "the creation date of the account", "value_description": "in the form YYMMDD"}, {"column_name": "district_id", "column_definition": "district_id INTEGER default 0 not null", "data_type": "INTEGER", "is_primary_key": false, "foreign_keys_info": [["district", "district_id"]], "referenced_by": [], "examples": [[18], [1], [5]], "column_description": "", "value_description": ""}], "table_primary_keys": ["account_id INTEGER default 0 not null primary key"], "table_foreign_keys": ["foreign key (district_id) references district (district_id)"], "unique_constraints": []}}}
{"set_translations": {"columns": [{"column_name": "setCode", "column_definition": "setCode TEXT REFERENCES sets (code) ON UPDATE CASCADE ON DELETE CASCADE", "data_type": "TEXT", "is_primary_key": false, "foreign_keys_info": [["sets", "code"]], "referenced_by": [], "examples": [["10E"], ["4ED"], ["5DN"]], "column_description": "", "value_description": ""}, {"column_name": "language", "column_definition": "language TEXT", "data_type": "TEXT", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [], "examples": ["Japanese"], "column_description": "", "value_description": ""}], "table_primary_keys": ["id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT"], "table_foreign_keys": [], "unique_constraints": []}, "sets": {"columns": [{"column_name": "code", "column_definition": "code TEXT not null unique", "data_type": "TEXT", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [["set_translations", "setCode"]], "examples": [["10E"], ["2ED"], ["2XM"]], "column_description": "", "value_description": ""}, {"column_name": "mtgoCode", "column_definition": "mtgoCode TEXT", "data_type": "TEXT", "is_primary_key": false, "foreign_keys_info": [], "referenced_by": [], "examples": [["10E"], ["2XM"], ["5DN"]], "column_description": "The set code for the set as it appears on Magic: The Gathering Online", "value_description": "if the value is null or empty, then it doesn't appear on Magic: The Gathering Online"}], "table_primary_keys": ["id INTEGER not null primary key autoincrement"], "table_foreign_keys": [], "unique_constraints": []}}}


entities：从用户query中提取出来的与数据库表名、列名、值相关的实体，
如果entity_type是表，则column和cell_value字段为null
如果entity_type是列，则cell_value字段为null

示例：

[
  {"entity": "movie title", "entity_type": "column", "table": "movies", "column": "movie_title", "cell_value": null},
  {"entity": "popularity", "entity_type": "column", "table": "movies", "column": "movie_popularity", "cell_value": null},
  {"entity": "movies", "entity_type": "table", "table": "movies", "column": null, "cell_value": null},
  {"entity": "1945", "entity_type": "cell_value", "table": "movies", "column": "movie_release_year", "cell_value": "1945"}
]

我想做的是一个隐私保护函数，所以我希望实现的功能是，对structured_db_schema和query都进行掩码处理。你帮我实现下这个函数。
该函数需要将structured_db_schema里的数据库结构信息，包括表名、列名、值、等等schema信息全部掩码，同时，也需要把query里提到的表、列、值也一对一掩码。方便我将掩码后的schema和query组织成一个掩码版本的nl2sql prompt。
而entities参数则是给你参考如何同步替换schema和query中的表名、列名、值的。
'''    

