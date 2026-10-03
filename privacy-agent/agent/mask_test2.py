import re
import json

def generate_schema_string_from_masked(masked_schema):
    """模拟外部依赖"""
    return json.dumps(masked_schema, ensure_ascii=False, indent=2)

class MaskerTest:
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

# --- 执行测试 ---
import json  # 确保导入了 json 模块

# 假设 MaskerTest 类在其他地方已定义
# from your_module import MaskerTest 

if __name__ == "__main__":
    
    # 1. 你提供的错误案例 Query 和 Entities
    query = "Among all chemical compounds identified in the database, what percent of compounds form a triple-bond. triple bond refers to bond_type = '#';"
    
    entities = [
        {
          "entity": "chemical compounds",
          "entity_type": "table",
          "table": "molecule",
          "column": "",
          "cell_value": ""
        },
        {
          "entity": "triple bond",
          "entity_type": "cell_value",
          "table": "bond",
          "column": "bond_type",
          "cell_value": "#"
        },
        {
          "entity": "bond",
          "entity_type": "table",
          "table": "bond",
          "column": "",
          "cell_value": ""
        },
        {
          "entity": "molecule_id",
          "entity_type": "column",
          "table": "bond",
          "column": "molecule_id",
          "cell_value": ""
        }
    ]
    
    # 2. 修正后的 Schema (将小写的 true/false 替换为大写的 True/False)
    structured_db_schema = {
        "molecule": {
            "columns": [
                {
                    "column_name": "molecule_id", 
                    "column_definition": "`molecule_id` TEXT NOT NULL", 
                    "data_type": "TEXT", 
                    "is_primary_key": True,  # 修复: true -> True
                    "foreign_keys_info": [], 
                    "referenced_by": [["atom", "molecule_id"], ["bond", "molecule_id"]], 
                    "examples": [["TR000"], ["TR001"], ["TR002"]], 
                    "column_description": "unique id of molecule", 
                    "value_description": "\"+\" --> this molecule / compound is carcinogenic '-' this molecule is not / compound carcinogenic"
                }
            ], 
            "table_primary_keys": ["PRIMARY KEY (`molecule_id`)"], 
            "table_foreign_keys": [], 
            "unique_constraints": []
        }, 
        "bond": {
            "columns": [
                {
                    "column_name": "molecule_id", 
                    "column_definition": "`molecule_id` TEXT DEFAULT NULL", 
                    "data_type": "TEXT", 
                    "is_primary_key": False,  # 修复: false -> False
                    "foreign_keys_info": [["molecule", "molecule_id"]], 
                    "referenced_by": [], 
                    "examples": [["TR000"], ["TR001"], ["TR002"]], 
                    "column_description": "identifying the molecule in which the bond appears", 
                    "value_description": ""
                }, 
                {
                    "column_name": "bond_type", 
                    "column_definition": "`bond_type` TEXT DEFAULT NULL", 
                    "data_type": "TEXT", 
                    "is_primary_key": False,  # 修复: false -> False
                    "foreign_keys_info": [], 
                    "referenced_by": [], 
                    "examples": ["-", "=", "#"], 
                    "column_description": "", 
                    "value_description": ""
                }, 
                {
                    "column_name": "bond_id", 
                    "column_definition": "`bond_id` TEXT NOT NULL", 
                    "data_type": "", 
                    "is_primary_key": False,  # 修复: false -> False
                    "foreign_keys_info": [], 
                    "referenced_by": [], 
                    "examples": [], 
                    "column_description": "", 
                    "value_description": ""
                }
            ], 
            "table_primary_keys": ["PRIMARY KEY (`bond_id`)"], 
            "table_foreign_keys": ["FOREIGN KEY (`molecule_id`) REFERENCES `molecule` (`molecule_id`)"], 
            "unique_constraints": []
        }
    }

    # 3. 运行测试
    tester = MaskerTest()
    output = tester._tool_apply_typed_masking(query, entities, structured_db_schema)

    # 4. 打印结果
    print("=== 脱敏后的查询语句 (Masked Query) ===")
    print(output["masked_query"])
    print("\n=== 解码字典 (Unmask Dict) ===")
    print(json.dumps(output["unmask_dict"], indent=2, ensure_ascii=False))