
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[2]))

import os
import json
import logging
from openai import OpenAI
from typing import Dict, List, Any, TypedDict, Optional
import re

# 导入你已有的模版文件
from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template
from template.sql_generate_template import get_sql_generate_template

from tools import generate_schema_string_from_masked
from langgraph.graph import StateGraph, END

# 配置日志格式，让输出的层级更清晰
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =================【1. 定义 LangGraph 全局状态 (State)】=================
class AgentState(TypedDict, total=False):
    # 输入信息
    original_question: str
    db_schema: str
    evidence: str
    structured_db_schema: dict
    case_id: str
    
    # 状态黑板 (Blackboard)
    rewritten_query: str
    entities: list
    masked_schema_str: str
    masked_query: str
    masked_sql: str
    unmask_dict: dict
    unmasked_sql: str
    last_thought: str
    
    # 控制流与图路由
    last_tool_name: str
    iteration: int
    final_result: str
    pending_tool_call: Optional[dict]


# =================【2. 隐私保护智能体类定义】=================
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
敏感数据定义：只要自然语言查询中存在与数据库的表、列、值相关的实体，则均当作敏感数据。

请遵循以下思考链：
1. 由于替换query中的实体可能会破坏句子结构，所以你需要先分析用户的原Query，评估其结构，是否能够直接对其隐身实体进行替换。如果存在直接替换隐私实体会破坏句法结构的情况，请调用 `query_rewrite` 工具进行改写。
2. 使用最终决定使用的 Query（原句或改写后的句子），调用 `entity_extract` 工具提取所有的隐私实体。
3. 拿到实体列表后，调用 `apply_typed_masking` 工具，将query进行掩码操作。
4. 评估掩码后的query和schema是否合理。如果不合理（比如漏掩码），你需要将遗漏的实体补充进去并再次调用apply_typed_masking。
5. 将掩码的query与schema传递给 `sql_generate` 工具进行SQL语句生成。
6. 拿到生成的掩码SQL后，调用 `sql_unmask` 工具，将其还原为真实的SQL语句，作为最终结果输出。
"""
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "query_rewrite",
                    "description": "当原query表述不规范时调用此工具。",
                    "parameters": {"type": "object", "properties": {"query_to_rewrite": {"type": "string"}}, "required": ["query_to_rewrite"]}
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "entity_extract",
                    "description": "提取 Schema Linking 实体信息数组。",
                    "parameters": {"type": "object", "properties": {"query_to_extract": {"type": "string"}}, "required": ["query_to_extract"]}
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "apply_typed_masking",
                    "description": "在底层同时对 Query 和 Structured Schema 执行同频表、列、值掩码替换。",
                    "parameters": {"type": "object", "properties": {"query": {"type": "string"}, "entities": {"type": "array", "items": {"type": "object"}}}, "required": ["query", "entities"]}
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "sql_generate",
                    "description": "基于掩码后的查询(masked_query)和掩码后的Schema(masked_schema)生成最终的SQL语句。在apply_typed_masking返回结果后使用。",
                    "parameters": {"type": "object", "properties": {"masked_query": {"type": "string"}, "masked_schema_str": {"type": "string"}}, "required": ["masked_query", "masked_schema_str"]}
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "sql_unmask",
                    "description": "将掩码后的SQL还原为真实的SQL语句。在sql_generate返回结果后使用。",
                    "parameters": {"type": "object", "properties": {"masked_sql": {"type": "string"}, "unmask_dict": {"type": "object"}}, "required": ["masked_sql", "unmask_dict"]}
                }
            }
        ]

        # =================【核心：构建 LangGraph 工作流】=================
        workflow = StateGraph(AgentState)
        
        # 1. 注册节点
        workflow.add_node("agent_reasoning", self.agent_node)
        workflow.add_node("tool_execution", self.tool_node)
        
        # 2. 设置起点
        workflow.set_entry_point("agent_reasoning")
        
        # 3. 设置条件路由 (大模型决策后走向哪里)
        workflow.add_conditional_edges(
            "agent_reasoning",
            self.should_continue,
            {
                "tool_execution": "tool_execution", # 调用工具
                "agent_reasoning": "agent_reasoning", # 没调工具但没结束，循环逼问
                "end": END # 流程彻底结束
            }
        )
        
        # 4. 工具执行完毕后，回传给大模型节点
        workflow.add_edge("tool_execution", "agent_reasoning")
        
        # 5. 编译成应用
        self.app = workflow.compile()

    # =================【内部日志工具】=================
    def _log_to_file(self, case_id: str, text: str):
        log_dir = "case_logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f"case_{case_id}.txt")
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(text + "\n")

    # =================【图节点 1: Agent 推理节点】=================
    def agent_node(self, state: AgentState) -> dict:
        case_id = state["case_id"]
        iteration = state.get("iteration", 0) + 1
        last_tool_name = state.get("last_tool_name", "init")
        
        logging.info(f"🔄 [第 {iteration} 轮推理思考中...] (当前所处节点: {last_tool_name})")
        
        if iteration > 20:
            self._log_to_file(case_id, f"\n{'='*30} [CASE TIMEOUT] {'='*30}\nAgent exceeded maximum iterations.")
            return {"final_result": '{"error": "Agent timeout"}', "iteration": iteration}

        # --- 动态拼装提示词 (还原你原有的逻辑) ---
        step_desc = ""
        content = ""
        original_question = state["original_question"]
        evidence = state["evidence"]
        db_schema = state["db_schema"]
        
        if last_tool_name == "init":
            step_desc = "步骤1：分析原始Query的句式结构，决定是否需要调用 query_rewrite 工具进行改写。如果可以直接替换实体而不破坏语法，请直接调用 entity_extract 工具提取实体。"
            content = f"【原始Schema】\n{db_schema}\n\n【原始用户问题】\n{original_question} {evidence}"
            
        elif last_tool_name == "query_rewrite":
            step_desc = "步骤1.5：评估改写后的Query是否合理。如果合理，请调用 entity_extract 工具提取实体；如果不合理，请重新调用 query_rewrite 工具进行改写。"
            content = f"【原始Schema】\n{db_schema}\n\n【原始用户问题】\n{original_question} {evidence}\n\n【改写后的Query】\n{state.get('rewritten_query', '')}"
            
        elif last_tool_name == "entity_extract":
            step_desc = "步骤2：实体提取已完成，准备进行掩码操作。请调用 apply_typed_masking 工具。"
            q = state.get('rewritten_query') if state.get('rewritten_query') else f"{original_question} {evidence}"
            content = f"【当前使用的Query】\n{q}\n\n【提取出的实体(Entities)】\n{json.dumps(state.get('entities', []), ensure_ascii=False)}"
            
        elif last_tool_name == "apply_typed_masking":
            step_desc = "步骤3：评估掩码效果。如果有实体遗漏（未被替换为mask），说明上一步提取不全，请手动把遗漏的实体补充进 entities 参数，然后再次调用 apply_typed_masking。如果完全合理，请调用 sql_generate 工具生成SQL。"
            content = f"【原始Schema】\n{db_schema}\n\n【原始用户问题】\n{original_question} {evidence}\n\n【掩码后的Schema】\n{state.get('masked_schema_str', '')}\n\n【掩码后的Query】\n{state.get('masked_query', '')}"
            
        elif last_tool_name == "sql_generate":
            step_desc = "步骤4：脱敏SQL已经生成。请调用 sql_unmask 工具，将其还原为真实的SQL语句。"
            content = f"【掩码后的SQL】\n{state.get('masked_sql', '')}\n\n【反向映射字典(unmask_dict)】\n{json.dumps(state.get('unmask_dict', {}), ensure_ascii=False)}"
            
        elif last_tool_name == "sql_unmask":
            step_desc = "步骤5：流程全部完成！请不要再调用任何工具，直接输出下方还原后的真实SQL语句。"
            content = f"【还原后的真实SQL】\n{state.get('unmasked_sql', '')}"
            
        # 防止模型进入死循环
        if state.get("last_thought"):
            content += f"\n\n【系统提醒】：在上一轮中，你只输出了文本没有调用工具：\n\"{state['last_thought']}\"\n请务必根据当前步骤调用相应工具继续推进！"
            
        user_content = f"【当前步骤指示】\n{step_desc}\n\n{content}\n\n请根据指示执行下一步。"
        
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_content}
        ]
        
        # 写入 INPUT 日志
        self._log_to_file(case_id, f"\n{'='*20} [Round {iteration} INPUT] {'='*20}")
        for m_idx, msg in enumerate(messages):
            self._log_to_file(case_id, f"--- Msg {m_idx + 1}: [{msg['role'].upper()}] ---")
            self._log_to_file(case_id, f"{msg['content'].strip()}")
        self._log_to_file(case_id, f"{'-'*55}\n")
        
        # --- 调用大模型 ---
        response = self.client.chat.completions.create(
            model=self.model_name, messages=messages, tools=self.tools, tool_choice="auto", temperature=self.temperature
        )
        message = response.choices[0].message
        
        # 写入 OUTPUT 日志
        self._log_to_file(case_id, f"{'='*20} [Round {iteration} OUTPUT] {'='*20}")
        if message.content:
            self._log_to_file(case_id, f"[Agent Thought]:\n{message.content.strip()}\n")
        else:
            self._log_to_file(case_id, f"[Agent Thought]: (无文本输出，直接调用工具)\n")
        
        # --- 解析工具调用 ---
        parsed_tool_calls = []
        if message.content:
            matches = re.finditer(r'<tool_call>\s*(.*?)\s*</tool_call>', message.content, re.DOTALL)
            for match in matches:
                try:
                    tool_data = json.loads(match.group(1))
                    parsed_tool_calls.append({"name": tool_data.get("name"), "arguments": tool_data.get("arguments", {})})
                except json.JSONDecodeError:
                    pass
        
        calls_to_process = []
        if message.tool_calls:
            calls_to_process = [{"name": tc.function.name, "arguments": json.loads(tc.function.arguments) if isinstance(tc.function.arguments, str) else tc.function.arguments} for tc in message.tool_calls]
        elif parsed_tool_calls:
            calls_to_process = parsed_tool_calls

        # --- 返回更新的 State ---
        if calls_to_process:
            return {
                "iteration": iteration,
                "pending_tool_call": calls_to_process[0],  # 将解析到的工具发给工具节点
                "last_thought": "" 
            }
        else:
            if last_tool_name == "sql_unmask":
                logging.info(f"🏁 任务结束，输出最终结果。")
                self._log_to_file(case_id, f"\n{'='*30} [CASE END] {'='*30}")
                self._log_to_file(case_id, f"[Final Answer]:\n{message.content}\n")
                return {
                    "iteration": iteration,
                    "final_result": message.content,
                    "pending_tool_call": None
                }
            else:
                return {
                    "iteration": iteration,
                    "last_thought": message.content or "",
                    "pending_tool_call": None
                }

    # =================【图边逻辑: 决定走向】=================
    def should_continue(self, state: AgentState) -> str:
        if state.get("final_result") or state.get("iteration", 0) > 20:
            return "end"
        
        if state.get("pending_tool_call"):
            return "tool_execution"
            
        # 如果模型没有给出工具，且流程还没结束，强制流转回 Agent 继续推理
        return "agent_reasoning"

    # =================【图节点 2: 工具执行节点】=================
    def tool_node(self, state: AgentState) -> dict:
        tc_data = state["pending_tool_call"]
        func_name = tc_data["name"]
        args = tc_data["arguments"]
        case_id = state["case_id"]
        
        log_args_str = json.dumps(args, ensure_ascii=False, indent=2)
        logging.info(f"👉 决定调用工具: [{func_name}]")
        self._log_to_file(case_id, f"[Action - Tool Call]: {func_name}")
        self._log_to_file(case_id, f"[Action - Arguments]:\n{log_args_str}\n")
        
        state_updates = {
            "last_tool_name": func_name,
            "pending_tool_call": None # 执行完后清空挂起任务
        }
        
        try:
            if func_name == "query_rewrite":
                result = self._tool_query_rewrite(state["db_schema"], args.get('query_to_rewrite', ''), case_id)
                state_updates['rewritten_query'] = result
                
            elif func_name == "entity_extract":
                result = self._tool_entity_extract(state["db_schema"], args.get('query_to_extract', ''), case_id)
                state_updates['entities'] = json.loads(result)
                
            elif func_name == "apply_typed_masking":
                result = self._tool_apply_typed_masking(
                    args.get('query', ''), 
                    args.get('entities', []), 
                    state["structured_db_schema"]
                )
                res_dict = json.loads(result)
                state_updates['masked_query'] = res_dict.get('masked_query', '')
                state_updates['masked_schema_str'] = res_dict.get('masked_schema_str', '')
                state_updates['unmask_dict'] = res_dict.get('unmask_dict', {})
                
            elif func_name == "sql_generate":
                result = self._tool_sql_generate(
                    args.get('masked_query', ''),
                    args.get('masked_schema_str', ''),
                    case_id
                )
                state_updates['masked_sql'] = result
                
            elif func_name == "sql_unmask":
                result = self._tool_sql_unmask(
                    args.get('masked_sql', ''),
                    args.get('unmask_dict', {})
                )
                state_updates['unmasked_sql'] = result
                
            logging.info(f"✅ 工具 [{func_name}] 执行完毕")
            self._log_to_file(case_id, f"[Action - Result]: (State Updated Successfully)\n")
            
        except Exception as e:
            logging.error(f"❌ 工具执行出错: {str(e)}")
            self._log_to_file(case_id, f"[Action - Error]: {str(e)}\n")
            
        return state_updates

    # =================【图入口：替代原来的 process】=================
    def process(self, item: Dict[str, Any], case_id: str = "default") -> str:
        # 初始化清理文件
        log_dir = "case_logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f"case_{case_id}.txt")
        open(log_file_path, 'w', encoding='utf-8').close()

        original_question = item['question']
        db_schema = item.get('db_schema', '') 
        evidence = item.get("evidence", '')
        structured_db_schema = item.get('structured_db_schema', {}) 
        
        self._log_to_file(case_id, f"{'='*30} [CASE START: {case_id}] {'='*30}")
        self._log_to_file(case_id, f"[USER QUESTION]:\n{original_question}\n")
        self._log_to_file(case_id, f"[EVIDENCE]:\n{evidence}\n")
        self._log_to_file(case_id, f"{'-'*80}\n")
        
        logging.info("="*50)
        logging.info(f"🚀 开始处理新 Query: {original_question} (Case ID: {case_id})")

        # 触发 LangGraph 的调用
        initial_state = {
            "original_question": original_question,
            "db_schema": db_schema,
            "evidence": evidence,
            "structured_db_schema": structured_db_schema,
            "case_id": case_id,
            "last_tool_name": "init",
            "iteration": 0,
        }
        
        # Invoke会跑完图的整个生命周期并返回最终状态字典
        final_state = self.app.invoke(initial_state)
        
        return final_state.get("final_result", '{"error": "Agent failed or timeout"}')

    # =================【内部具体工具方法映射 (与你原始代码一致)】=================
    def _tool_query_rewrite(self, db_schema: str, query_to_rewrite: str, case_id: str) -> str:
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_rewrite}"
        messages = get_query_rewrite_messages(input_str)
        self._log_to_file(case_id, f"    >>> [Inner Tool - query_rewrite INPUT] >>>\n{json.dumps(messages, ensure_ascii=False, indent=2)}\n")
        response = self.client.chat.completions.create(model=self.model_name, messages=messages, temperature=self.temperature)
        ans = response.choices[0].message.content.strip()
        self._log_to_file(case_id, f"    <<< [Inner Tool - query_rewrite OUTPUT] <<<\n{ans}\n")
        return ans

    def _tool_entity_extract(self, db_schema: str, query_to_extract: str, case_id: str) -> str:
        input_str = f"- Database Schema:\n{db_schema}\n- User's Question:\n{query_to_extract}"
        messages = get_entity_extract_template(input_str)
        self._log_to_file(case_id, f"    >>> [Inner Tool - entity_extract INPUT] >>>\n{json.dumps(messages, ensure_ascii=False, indent=2)}\n")
        response = self.client.chat.completions.create(model=self.model_name, messages=messages, temperature=self.temperature)
        response_text = response.choices[0].message.content.strip()
        self._log_to_file(case_id, f"    <<< [Inner Tool - entity_extract OUTPUT] <<<\n{response_text}\n")
        match = re.search(r'\[\s*\{.*?\}\s*\]', response_text, re.DOTALL)
        return match.group(0) if match else "[]"
    
    def _tool_sql_generate(self, masked_query, masked_schema_str, case_id: str):
        input_str = f"- Database Schema:\n{masked_schema_str}\n- User's Question:\n{masked_query}"
        messages = get_sql_generate_template(input_str)
        self._log_to_file(case_id, f"    >>> [Inner Tool - sql_generate INPUT] >>>\n{json.dumps(messages, ensure_ascii=False, indent=2)}\n")
        response = self.client.chat.completions.create(model=self.model_name, messages=messages, temperature=self.temperature)
        response_text = response.choices[0].message.content.strip()
        self._log_to_file(case_id, f"    <<< [Inner Tool - sql_generate OUTPUT] <<<\n{response_text}\n")
        return response_text

    # 下方两个非 LLM 调用工具完全保持原样，无需 log 穿透
    def _tool_sql_unmask(self, masked_sql: str, unmask_dict: Any) -> str:
        if isinstance(unmask_dict, str):
            try: unmask_dict = json.loads(unmask_dict)
            except Exception: pass
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
        table_map, col_map, val_map = {}, {}, {}
        t_idx, c_idx, v_idx = 1, 1, 1
        
        for table_name, table_info in structured_db_schema.items():
            if table_name not in table_map:
                table_map[table_name] = f"table_{t_idx}"
                t_idx += 1
            cols = table_info["columns"] if "columns" in table_info else [{"column_name": k, **v} for k, v in table_info.items()]
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
            if not isinstance(text, str) or not text: return text
            replace_dict = {orig: mask for orig, mask in table_map.items()}
            replace_dict.update({orig_c: mask for (orig_t, orig_c), mask in col_map.items()})
            replace_dict.update({orig: mask for orig, mask in val_map.items()})
            sorted_keys = sorted(replace_dict.keys(), key=lambda k: len(str(k)), reverse=True)
            res = text
            for k in sorted_keys:
                k_str = str(k)
                if re.match(r'^\w+$', k_str): res = re.sub(r'\b' + re.escape(k_str) + r'\b', replace_dict[k], res, flags=re.IGNORECASE)
                else: res = res.replace(k_str, replace_dict[k])
            return res

        masked_query = query
        sorted_entities = sorted(entities, key=lambda x: len(str(x.get("entity", ""))), reverse=True)
        for ent in sorted_entities:
            ent_str = str(ent["entity"])
            mask = None
            if ent["entity_type"] == "table": mask = table_map.get(ent["table"])
            elif ent["entity_type"] == "column": mask = col_map.get((ent["table"], ent["column"]))
            elif ent["entity_type"] == "cell_value": mask = val_map.get(str(ent["cell_value"]))
                
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
                    for attr_key, attr_val in col_info.items(): m_col_info[attr_key] = replace_in_string(attr_val) if isinstance(attr_val, str) else attr_val
                    m_col_info["column_name"] = col_map.get((table_name, col_info["column_name"]), col_info["column_name"])
                    m_table_info["columns"].append(m_col_info)
                for k, v in table_info.items():
                    if k != "columns":
                        if isinstance(v, list): m_table_info[k] = [replace_in_string(item) if isinstance(item, str) else item for item in v]
                        else: m_table_info[k] = replace_in_string(v) if isinstance(v, str) else v
            else:
                for col_name, col_attrs in table_info.items():
                    m_col_name = col_map.get((table_name, col_name), col_name)
                    m_col_attrs = {}
                    for attr_key, attr_val in col_attrs.items(): m_col_attrs[attr_key] = replace_in_string(attr_val) if isinstance(attr_val, str) else attr_val
                    m_table_info[m_col_name] = m_col_attrs
            masked_schema[m_table_name] = m_table_info

        unmask_dict = {
            "tables": {mask: orig for orig, mask in table_map.items()},
            "columns": {mask: orig_c for (orig_t, orig_c), mask in col_map.items()},
            "values": {mask: orig for orig, mask in val_map.items()}
        }
        masked_schema_str = generate_schema_string_from_masked(masked_schema)
        
        return json.dumps({"masked_query": masked_query, "masked_schema_str": masked_schema_str, "unmask_dict": unmask_dict}, ensure_ascii=False)


# =================【主入口调用】=================
def read_jsonl_to_list(path, encoding="utf-8"):
    with open(path, "r", encoding=encoding) as f:
        return [json.loads(line) for line in f if line.strip()]
    
if __name__ == '__main__':
    BASE_URL = "https://api.deepseek.com/v1"  
    MODEL_NAME = 'deepseek-chat'
    API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

    agent = PrivacyProtectionAgent(base_url=BASE_URL, model_name=MODEL_NAME, api_key=API_KEY)
    
    data_path = str(_RELEASE_ROOT / 'privacy-agent/agent/data/bird_dev_sc.jsonl')
    data = read_jsonl_to_list(data_path)

    for idx, item in enumerate(data[1:5]):
        case_id = str(item.get("question_id", f"idx_{idx}"))
        final_result = agent.process(item, case_id=case_id)
        
        print(f"\n🟢 Agent Case {case_id} 最终输出:")
        print(final_result)
        print(f"📄 查看详情请打开: case_logs/case_{case_id}.txt\n")