import os
import re
import json
import sqlite3
import asyncio
import json_repair
from typing import Any, Optional
from openai import OpenAI
from openai import OpenAI, APITimeoutError, APIConnectionError

from verl.tools.base_tool import BaseTool
from verl.tools.schemas import ToolResponse, OpenAIFunctionToolSchema
from verl.utils.rollout_trace import rollout_trace_op

# 引入您项目中原有的模板和工具函数
from verl.tools.template.query_rewrite_template import get_query_rewrite_messages
from verl.tools.template.entity_extract_template import get_entity_extract_template
from verl.tools.template.sql_generate_template import get_sql_generate_template
from verl.tools.tools import generate_schema_string_from_masked, safe_execute_sql

async def run_blocking_with_timeout(func, timeout_sec: float, tool_name: str):
    """
    在线程中执行阻塞函数，并使用 asyncio.wait_for 做外层超时兜底。
    注意：wait_for 取消的是 asyncio 任务，底层线程无法被强杀，
    因此 OpenAI client 自身也必须设置 timeout。
    """
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(func),
            timeout=timeout_sec,
        )
    except asyncio.TimeoutError:
        raise TimeoutError(f"{tool_name} timed out after {timeout_sec} seconds")
    

class QueryRewriteTool(BaseTool):
    """
    查询重写工具：调用本地微调模型修复查询的句法结构
    """
    def __init__(self, config: dict, tool_schema: OpenAIFunctionToolSchema):
        super().__init__(config, tool_schema)

        base_url = self.config.get("base_url", os.getenv("QUERY_REWRITE_BASE_URL", "http://127.0.0.1:8036/v1"))
        self.model_name = self.config.get(
            "model_name",
            os.getenv("QUERY_REWRITE_MODEL_NAME", "query_rewrite_v2")
        )
        self.temperature = self.config.get("temperature", 0)

        # 新增：请求超时和重试次数
        self.request_timeout = float(self.config.get("request_timeout", 60.0))
        self.max_retries = int(self.config.get("max_retries", 0))

        self.client = OpenAI(
            api_key=self.config.get("api_key", "EMPTY"),
            base_url=base_url,
            timeout=self.request_timeout,
            max_retries=self.max_retries,
        )

    @rollout_trace_op
    async def execute(self, instance_id: str, parameters: dict[str, Any], **kwargs) -> tuple[ToolResponse, float, dict]:
        try:
            query_to_rewrite = parameters.get('query_to_rewrite', '')
            rewrite_instruction = parameters.get('rewrite_instruction', '')

            db_schema = kwargs.get("agent_data", {}).tools_kwargs.get(
                "query_rewrite", {}
            ).get("execute_kwargs", {}).get("db_schema", "")

            input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_rewrite}"
            if rewrite_instruction:
                input_str += f"\n- Rewrite Instruction:\n{rewrite_instruction}"

            messages = get_query_rewrite_messages(input_str)

            def _call_api():
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=self.temperature,
                    timeout=self.request_timeout,
                )
                return response.choices[0].message.content.strip()

            ans = await run_blocking_with_timeout(
                _call_api,
                timeout_sec=self.request_timeout + 5.0,
                tool_name="QueryRewriteTool",
            )

            return ToolResponse(text=ans), 0.2, {}

        except (TimeoutError, APITimeoutError) as e:
            return ToolResponse(text=f"QueryRewriteTool Timeout: {str(e)}"), 0.2, {"timeout": True}

        except APIConnectionError as e:
            return ToolResponse(text=f"QueryRewriteTool Connection Error: {str(e)}"), 0.2, {"connection_error": True}

        except Exception as e:
            error_msg = f"Error executing QueryRewriteTool: {str(e)}"
            return ToolResponse(text=error_msg), 0.0, {}
        



class EntityExtractTool(BaseTool):
    """
    实体抽取工具：调用本地微调模型提取 Query 中的隐私实体
    """
    def __init__(self, config: dict, tool_schema: OpenAIFunctionToolSchema):
        super().__init__(config, tool_schema)

        base_url = self.config.get("base_url", os.getenv("ENTITY_EXTRACT_BASE_URL", "http://127.0.0.1:8037/v1"))
        self.model_name = self.config.get(
            "model_name",
            os.getenv("ENTITY_EXTRACT_MODEL_NAME", "entity_extract_v1")
        )
        self.temperature = self.config.get("temperature", 0)

        self.request_timeout = float(self.config.get("request_timeout", 60.0))
        self.max_retries = int(self.config.get("max_retries", 0))

        self.client = OpenAI(
            api_key=self.config.get("api_key", "EMPTY"),
            base_url=base_url,
            timeout=self.request_timeout,
            max_retries=self.max_retries,
        )

    @rollout_trace_op
    async def execute(self, instance_id: str, parameters: dict[str, Any], **kwargs) -> tuple[ToolResponse, float, dict]:
        try:
            query_to_extract = parameters.get('query_to_extract', '')

            db_schema = kwargs.get("agent_data", {}).tools_kwargs.get(
                "entity_extract", {}
            ).get("execute_kwargs", {}).get("db_schema", "")

            input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_extract}"
            messages = get_entity_extract_template(input_str)

            def _call_api():
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=self.temperature,
                    timeout=self.request_timeout,
                )
                return response.choices[0].message.content.strip()

            response_text = await run_blocking_with_timeout(
                _call_api,
                timeout_sec=self.request_timeout + 5.0,
                tool_name="EntityExtractTool",
            )

            match = re.search(r'\[\s*\{.*?\}\s*\]', response_text, re.DOTALL)
            result = match.group(0) if match else "[]"

            return ToolResponse(text=result), 0.2, {}

        except (TimeoutError, APITimeoutError) as e:
            return ToolResponse(text=f"EntityExtractTool Timeout: {str(e)}"), 0.2, {"timeout": True}

        except APIConnectionError as e:
            return ToolResponse(text=f"EntityExtractTool Connection Error: {str(e)}"), 0.2, {"connection_error": True}

        except Exception as e:
            error_msg = f"Error executing EntityExtractTool: {str(e)}"
            return ToolResponse(text=error_msg), 0.0, {}
        


class ApplyTypedMaskingTool(BaseTool):
    """
    脱敏工具：对 Query 和 Structured Schema 进行底层替换掩码操作
    """
    
    @rollout_trace_op
    async def execute(self, instance_id: str, parameters: dict[str, Any], **kwargs) -> tuple[ToolResponse, float, dict]:
        try:
            # 从模型生成的参数中获取 query 和 entities
            query = parameters.get('query', '')
            entities = parameters.get('entities', [])
            
            # structured_db_schema 通常由外部环境维护，建议从 kwargs 传入
            # 如果您的设计是让大模型直接传，也可以改为 parameters.get('structured_db_schema', {})
            structured_db_schema = kwargs.get("agent_data", {}).tools_kwargs.get("apply_typed_masking", {}).get("execute_kwargs", {}).get("structured_db_schema", {})

            if not structured_db_schema:
                return ToolResponse(text="Error: Missing structured_db_schema in context."), 0.0, {}

            # ==========================================
            # 以下为您提供的核心脱敏逻辑
            # ==========================================
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
            
            # 调用外部依赖函数生成 Schema String
            masked_schema_str = generate_schema_string_from_masked(masked_schema)
            
            res_dict = {
                "masked_query": masked_query, 
                "masked_schema_str": masked_schema_str, 
                "unmask_dict": unmask_dict
            }
            
            # ==========================================
            # 执行成功且无语法错误：奖励 0.2
            # ==========================================
            return ToolResponse(text=json.dumps(res_dict, ensure_ascii=False)), 0.2, res_dict
            
        except Exception as e:
            # ==========================================
            # 出现任何异常报错：奖励 0.0
            # ==========================================
            error_msg = f"Error executing ApplyTypedMaskingTool: {str(e)}"
            return ToolResponse(text=error_msg), 0.0, {}



class SQLGenerateTool(BaseTool):
    """
    SQL 生成工具：根据脱敏后的 Schema 和 Query，生成脱敏版 SQL
    """
    def __init__(self, config: dict, tool_schema: OpenAIFunctionToolSchema):
        super().__init__(config, tool_schema)

        base_url = self.config.get("base_url", os.getenv("SQL_GENERATE_BASE_URL", "http://127.0.0.1:8039/v1"))
        self.model_name = self.config.get("model_name", os.getenv("SQL_GENERATE_MODEL_NAME", "Qwen3.6-27B"))
        self.api_key = self.config.get("api_key", os.getenv("SQL_GENERATE_API_KEY", "EMPTY"))
        self.temperature = self.config.get("temperature", 0)

        self.request_timeout = float(self.config.get("request_timeout", 120.0))
        self.max_retries = int(self.config.get("max_retries", 0))

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=base_url,
            timeout=self.request_timeout,
            max_retries=self.max_retries,
        )

    @rollout_trace_op
    async def execute(self, instance_id: str, parameters: dict[str, Any], **kwargs) -> tuple[ToolResponse, float, dict]:
        try:
            masked_query = parameters.get('masked_query', '')
            masked_schema_str = parameters.get('masked_schema_str', '')

            input_str = f"- Database Schema:\n{masked_schema_str}\n- User's Question:\n{masked_query}"
            messages = get_sql_generate_template(input_str)

            def _call_api():
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=self.temperature,
                    timeout=self.request_timeout,
                    extra_body={
                        "chat_template_kwargs": {"enable_thinking": False}
                    },
                )
                return response.choices[0].message.content.strip()

            result = await run_blocking_with_timeout(
                _call_api,
                timeout_sec=self.request_timeout + 5.0,
                tool_name="SQLGenerateTool",
            )

            return ToolResponse(text=result), 0.2, {}

        except (TimeoutError, APITimeoutError) as e:
            return ToolResponse(text=f"SQLGenerateTool Timeout: {str(e)}"), 0.2, {"timeout": True}

        except APIConnectionError as e:
            return ToolResponse(text=f"SQLGenerateTool Connection Error: {str(e)}"), 0.2, {"connection_error": True}

        except Exception as e:
            error_msg = f"Error executing SQLGenerateTool: {str(e)}"
            return ToolResponse(text=error_msg), 0.0, {}


class SQLUnmaskTool(BaseTool):
    """
    SQL 还原工具：将模型生成的脱敏 SQL 根据字典还原为包含真实数据库名称的 SQL
    """
    
    @rollout_trace_op
    async def execute(self, instance_id: str, parameters: dict[str, Any], **kwargs) -> tuple[ToolResponse, float, dict]:
        try:
            # 从模型传入的参数中获取 masked_sql 和 unmask_dict
            masked_sql = parameters.get('masked_sql', '')
            unmask_dict = parameters.get('unmask_dict', {})

            # ==========================================
            # 以下为您提供的核心还原逻辑
            # ==========================================
            if isinstance(unmask_dict, str):
                try: 
                    unmask_dict = json_repair.loads(unmask_dict)
                except Exception: 
                    pass
            
            # 如果参数非法或者为空，当做失败处理，奖励 0.0
            if not masked_sql or not isinstance(masked_sql, str) or not isinstance(unmask_dict, dict):
                return ToolResponse(text=str(masked_sql)), 0.0, {}
                
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
                
            # ==========================================
            # 执行成功，正常还原出 SQL：奖励 0.2
            # ==========================================
            return ToolResponse(text=unmasked_text), 0.2, {}
            
        except Exception as e:
            # ==========================================
            # 出现任何正则、解析等异常报错：奖励 0.0
            # ==========================================
            error_msg = f"Error executing SQLUnmaskTool: {str(e)}"
            return ToolResponse(text=error_msg), 0.0, {}


class SQLExecuteCheckTool(BaseTool):
    """SQL 数据库执行检查工具"""
    
    @rollout_trace_op
    async def execute(self, instance_id: str, parameters: dict[str, Any], **kwargs) -> tuple[ToolResponse, float, dict]:
        try:
            unmasked_sql = parameters.get('unmasked_sql', '')
            db_path = kwargs.get("agent_data", {}).tools_kwargs.get("execute_sql_check", {}).get("execute_kwargs", {}).get("db_path", "")
            
            if not unmasked_sql or not db_path:
                return ToolResponse(text="Error: Missing SQL or Database Path."), 0.0, {}
            
            def _execute_check():
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
                    if 'conn' in locals():
                        conn.close()

            result_text = await asyncio.to_thread(_execute_check)
            
            # 判断结果：只有 "Success" 代表完全没有语法和执行错误，奖励 0.2，否则奖励 0
            if result_text == "Success":
                reward = 0.2
            else:
                reward = 0.0
                
            return ToolResponse(text=result_text), reward, {}
        except Exception as e:
            # 内部崩溃防守
            return ToolResponse(text=f"Error executing SQLExecuteCheckTool: {str(e)}"), 0.0, {}
