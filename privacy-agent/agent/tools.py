
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[2]))

from template.query_rewrite_template import get_query_rewrite_messages
from template.entity_extract_template import get_entity_extract_template

from openai import OpenAI
import json 
def openai_api(messages, base_url, model_name, api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"), temperature=0.0,max_tokens=4096):
    client = OpenAI(
        api_key=api_key,  
        base_url=base_url
    )
    model_name = model_name
    response = client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=0.0,
            max_tokens=4096
        )
    return response.choices[0].message.content

def query_rewrite(item):
    base_url = "http://127.0.0.1:2333/v1"  
    api_key = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")
    model_name = '/root/models/Qwen3-235B-A22B-Instruct'

    input_str = "- Database Schema:\n" + item['db_schema'] + "\n- User's Question:\n" + item['question']
    messages = get_query_rewrite_messages(input_str)
    rewrite_query = openai_api(messages, base_url, model_name)
    return rewrite_query

def entity_extract(item):
    base_url = "http://127.0.0.1:2333/v1"  
    api_key = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")
    model_name = '/root/models/Qwen3-235B-A22B-Instruct'

    input_str = "- Database Schema:\n" + item['db_schema'] + "\n- User's Question:\n" + item['question']
    messages = get_entity_extract_template(input_str)
    entity_list = openai_api(messages, base_url, model_name)
    return entity_list


def generate_schema_string_from_masked(masked_schema: dict, include_value_description: bool = True) -> str:
    """
    根据掩码后的字典 (masked_schema) 生成带有示例和描述的 DDL 字符串格式。
    
    Args:
        masked_schema (dict): 由 _tool_apply_typed_masking 导出的掩码 schema。
        include_value_description (bool): 是否在注释中包含字段值描述。
        
    Returns:
        str: 拼接好的 schema 字符串。
    """
    ddl_commands = []

    for table_name, table_info in masked_schema.items():
        schema_lines = [f"CREATE TABLE {table_name} ("]
        
        inner_lines = []
        
        # ==========================================
        # 1. 处理列定义 (Columns)
        # ==========================================
        for col_info in table_info.get("columns", []):
            # 获取列的原始定义 (如 'col_1 INTEGER null')
            col_def = col_info.get("column_definition", "")
            if not col_def:
                # 兜底：如果 definition 丢失，用 name 和 type 拼装
                col_def = f"{col_info.get('column_name')} {col_info.get('data_type', '')}".strip()

            # --- 拼装注释部分 (参考你提供的代码逻辑) ---
            example_part = ""
            description_part = ""
            
            # 提取示例
            examples = col_info.get("examples", [])
            if examples:
                flat_examples = []
                for ex in examples:
                    # 兼容数据库 cursor 返回的列表嵌套形式如 [[417], [505]] 和普通列表 [417, 505]
                    val = ex[0] if isinstance(ex, list) and len(ex) > 0 else ex
                    flat_examples.append(str(val))
                
                # 长度大于 256 的截断保护
                example_str = ", ".join([f"`{x}`" if len(x) < 256 else "null" for x in flat_examples])
                example_part = f" Example Values: {example_str}"

            # 提取描述
            desc_list = []
            col_desc = col_info.get("column_description", "")
            if col_desc:
                desc_list.append(f"Column Description: {col_desc}")
                
            val_desc = col_info.get("value_description", "")
            if val_desc and include_value_description:
                desc_list.append(f"Value Description: {val_desc}")
                
            if desc_list:
                description_part = " | " + " | ".join(desc_list)
            
            # 组合列定义和注释
            comment_str = ""
            # if example_part or description_part:
            #     comment_str = f" --{example_part}{description_part}"

            # 暂存该行内容 (行尾的逗号在后面统一处理)
            inner_lines.append({
                "definition": f"\t{col_def}",
                "comment": comment_str
            })

        # ==========================================
        # 2. 处理表级约束 (Primary/Foreign Keys, Unique)
        # ==========================================
        constraints = []
        for pk in table_info.get("table_primary_keys", []):
            constraints.append(f"\t{pk}")
        for fk in table_info.get("table_foreign_keys", []):
            constraints.append(f"\t{fk}")
        for uc in table_info.get("unique_constraints", []):
            constraints.append(f"\t{uc}")

        # 将约束和列合并，处理 SQL 的逗号分隔符
        total_items_count = len(inner_lines) + len(constraints)
        current_idx = 0
        
        # 渲染列
        for line_data in inner_lines:
            current_idx += 1
            # 如果不是最后一条（无论是列还是后续的约束），加上逗号
            comma = "," if current_idx < total_items_count else ""
            schema_lines.append(f"{line_data['definition']}{comma}{line_data['comment']}")
            
        # 渲染约束
        for constraint in constraints:
            current_idx += 1
            comma = "," if current_idx < total_items_count else ""
            schema_lines.append(f"{constraint}{comma}")

        schema_lines.append(");")
        
        # 组合当前表的完整 DDL
        ddl_commands.append("\n".join(schema_lines))

    return "\n\n".join(ddl_commands)


if __name__ == '__main__':
    data = json.load(open(str(_RELEASE_ROOT / 'privacy-agent/tools/bird.json'), 'r'))    
    for item in data:
        print('-=-------------------------------------')
        rewrite_result = query_rewrite(item)
        print(item['question'] + ' ' + item['evidence'])
        print(rewrite_result)
        print('-=-------------------------------------')
