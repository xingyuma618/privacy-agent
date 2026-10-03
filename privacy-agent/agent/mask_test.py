import copy
import re

def _tool_apply_typed_masking(query, entities, structured_db_schema):
    """
    对 NL2SQL 的 query 和 schema 进行隐私掩码处理 (适配了新的 Schema 字典结构)
    """
    # 1. 初始化映射字典和计数器
    table_map = {}
    column_map = {}
    value_map = {}
    
    t_count, c_count, v_count = 1, 1, 1
    
    def to_key(val):
        return str(val).strip()

    # --- 阶段一：收集映射 ---
    for table_name, table_data in structured_db_schema.items():
        if table_name not in table_map:
            table_map[table_name] = f"Table_{t_count}"
            t_count += 1
            
        # 注意：这里的 table_data 现在直接就是 {col_name: {col_info}} 的字典
        for col_name, col_info in table_data.items():
            # 过滤掉可能存在的表级约束键（如果有的话）
            if not isinstance(col_info, dict): 
                continue
                
            if col_name not in column_map:
                column_map[col_name] = f"Column_{c_count}"
                c_count += 1
                
            # 收集 example_values 中的值 (注意新数据里的键名变成了 example_values)
            for example in col_info.get("example_values", []):
                val = example[0] if isinstance(example, list) else example
                if to_key(val) not in value_map:
                    value_map[to_key(val)] = f"Value_{v_count}"
                    v_count += 1

    for entity in entities:
        if entity["entity_type"] == "cell_value" and entity["cell_value"] is not None:
            val_key = to_key(entity["cell_value"])
            if val_key not in value_map:
                value_map[val_key] = f"Value_{v_count}"
                v_count += 1

    def exact_replace(text, mapping):
        if not text: return text
        if not isinstance(text, str): return text
        sorted_keys = sorted(mapping.keys(), key=len, reverse=True)
        for key in sorted_keys:
            pattern = re.compile(r'\b' + re.escape(key) + r'\b', re.IGNORECASE)
            text = pattern.sub(mapping[key], text)
        return text

    # --- 阶段二：掩码 Schema ---
    masked_schema = {}
    schema_mapping = {**table_map, **column_map}
    
    for table_name, table_data in structured_db_schema.items():
        masked_table_name = table_map[table_name]
        masked_schema[masked_table_name] = {}
        
        for col_name, col_info in table_data.items():
            if not isinstance(col_info, dict):
                # 如果有不是列的额外信息（如 table_primary_keys），直接 copy
                masked_schema[masked_table_name][col_name] = copy.deepcopy(col_info)
                continue

            masked_col_name = column_map[col_name]
            masked_schema[masked_table_name][masked_col_name] = copy.deepcopy(col_info)
            current_col = masked_schema[masked_table_name][masked_col_name]
            
            # 替换 type / definition / description
            if "type" in current_col:
                current_col["type"] = exact_replace(current_col["type"], schema_mapping)
            if "description" in current_col:
                current_col["description"] = exact_replace(current_col["description"], schema_mapping)
            
            # 替换 example_values
            new_examples = []
            for example in current_col.get("example_values", []):
                if isinstance(example, list):
                    new_examples.append([value_map.get(to_key(example[0]), example[0])])
                else:
                    new_examples.append(value_map.get(to_key(example), example))
            current_col["example_values"] = new_examples

    # --- 阶段三：掩码 Query ---
    masked_query = query
    sorted_entities = sorted(entities, key=lambda x: len(x["entity"]), reverse=True)
    
    for ent in sorted_entities:
        original_text = ent["entity"]
        ent_type = ent["entity_type"]
        replacement = original_text
        
        if ent_type == "table" and ent["table"] in table_map:
            replacement = table_map[ent["table"]]
        elif ent_type == "column" and ent["column"] in column_map:
            replacement = column_map[ent["column"]]
        elif ent_type == "cell_value" and to_key(ent["cell_value"]) in value_map:
            replacement = value_map[to_key(ent["cell_value"])]
            
        masked_query = masked_query.replace(original_text, replacement)

    return masked_query, masked_schema, table_map, column_map, value_map

# ================= 测试部分 =================

# ================= 新的测试用例 =================

# 1. 准备输入参数
query = "List the movie_title of the movie whose movie_id appears most frequently in the ratings table (i.e., the movie with the highest count of rating rows)."

entities = [
    {"entity": "movie_title", "entity_type": "column", "table": "movies", "column": "movie_title", "cell_value": None}, 
    {"entity": "movies", "entity_type": "table", "table": "movies", "column": None, "cell_value": None}, 
    {"entity": "movie_id", "entity_type": "column", "table": "ratings", "column": "movie_id", "cell_value": None}, 
    {"entity": "ratings", "entity_type": "table", "table": "ratings", "column": None, "cell_value": None}
]

structured_db_schema = {
    "ratings": {
        "rating_id": {
            "type": "INTEGER", 
            "example_values": ["(1066,)", "(1067,)", "(1068,)"], 
            "description": "Movie ID related to the rating"
        }, 
        "movie_id": {
            "type": "INTEGER", 
            "example_values": ["(1066,)", "(1067,)", "(1068,)"], 
            "description": "Movie ID related to the rating"
        }
    }, 
    "movies": {
        "movie_id": {
            "type": "INTEGER", 
            "example_values": [], 
            "description": ""
        }, 
        "movie_title": {
            "type": "TEXT", 
            # 注意这里对 It's Winter 进行了转义处理
            "example_values": ["('La Antena',)", "('Elementary Particles',)", "('It\\'s Winter',)"], 
            "description": "Name of the movie"
        }
    }
}

# 2. 执行掩码函数 (注意参数顺序：query, entities, structured_db_schema)
masked_query, masked_schema, t_map, c_map, v_map = _tool_apply_typed_masking(query, entities, structured_db_schema)

# 3. 打印结果验证
print("=== 1. Masked Query (脱敏后的自然语言查询) ===")
print(masked_query)

print("\n=== 2. Masked Schema (脱敏后的数据库结构) ===")
import json
print(json.dumps(masked_schema, indent=2, ensure_ascii=False))

print("\n=== 3. Mapping Tables (映射字典，用于后续解析 SQL) ===")
print(f"Table Map:  {t_map}")
print(f"Column Map: {c_map}")
print(f"Value Map:  {v_map}")