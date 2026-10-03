import re 
import sqlite3
import threading  # 新增：用于计时器强行打断
import json_repair
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
    """
    【新增】使用 SQLite 原生的 interrupt() 机制来中断超时查询
    """
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
        timer.cancel() # 执行完毕后取消定时器
        cursor.close()

def execute_sql(predicted_sql, ground_truth, db_path, logging,timeout_sec=20.0):
    """
    【修改】执行 SQL 并对比结果，使用原生防卡死机制。
    """
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
        logging.warning(f"⚠️ SQL Execution timeout during evaluation.")
        return 0
    except Exception as e:
        logging.debug(f"SQL execution failed: {str(e)}")
        return 0
    finally:
        if 'conn' in locals():
            conn.close()


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












example_user1 = '''- Original Schema:
CREATE TABLE lists
(
	list_url TEXT, --  
	list_id INTEGER not null, --  
	FOREIGN KEY (user_id) REFERENCES lists_users (user_id),
	list_title TEXT, -- Example Values: `Location`, `Short and pretty damn sweet`  
	PRIMARY KEY (list_id),
);

- Original User Query:
Where can I find the movie list "Short and pretty damn sweet"? Short and pretty damn sweet is list_title; location of the movie refers to list_url;

- Masked Query:
Where can I find the movie list "val_1"? Short and pretty damn sweet is 'col_3'; location of the movie refers to 'col_1';
'''

example_assist1 = '''{
  "thought": "The masked query successfully replaces explicit column names with 'col_1' and 'col_3', and masks the first instance of the list title. However, it fails to mask the second occurrence of the sensitive database value 'Short and pretty damn sweet'. Additionally, it leaves the terms 'movie' and 'movie list' unmasked, which leaks the underlying business context and domain of the schema. Deduct 0.4 for leaking the specific database value and the business context.",
  "score": 0.6
}
'''

example_user2 = '''- Original Schema:
CREATE TABLE twitter
(
	TweetID TEXT, -- Example Values: `('tw-682712873332805633',)` | Column Description: the unique id of the tweet 
	UserID TEXT, -- Example Values: `('tw-40932430',)` 
	FOREIGN KEY (UserID) REFERENCES user(UserID),
	PRIMARY KEY (TweetID),
);

CREATE TABLE user
(
	UserID TEXT constraint user_pk primary key,
	Gender TEXT, -- Example Values: `Male`, `Unisex`, `Female`   
);
- Original User Query:
What is the gender of the user who posted a tweet with ID tw-682714583044243456?

- Masked Query:
What is the col_3 of the table_2 who posted a col_1 val_1? "val_1" is the col_1
'''

example_assist2 = '''{
  "thought": "The masked query successfully replaces all explicit column names ('gender'), table names ('user', 'tweet'), and specific database values ('tw-682714583044243456') with generic placeholders ('col_3', 'table_2', 'col_1', 'val_1'). No sensitive entities or underlying business context are leaked.",
  "score": 1.0
}
'''


def calculate_privacy_reward(client, model_name, original_query, masked_query, db_schema, logging) -> float:
        if not masked_query:
            return 0.0 # 连脱敏结果都没有，直接 0 分

        # 构建裁判模型的 Prompt
        judge_prompt = f"""You are an expert Data Privacy Evaluator. Your task is to evaluate whether a natural language query and a database schema have been properly desensitized (masked), ensuring no real sensitive database entities are leaked.

[Definition of Sensitive Entities]
All database column names, table names, and values related to the Original Schema appearing in the query are considered sensitive entities.

[Scoring Criteria]
- Perfect (1.0): No sensitive entities from the original data are leaked in the masked query.
- Partial/Poor (< 1.0): Deduct 0.2 points for EVERY distinct sensitive entity that is leaked (e.g., if the real table name and a real person's name are both leaked, deduct 0.4. Score = 0.6).
- Minimum score is 0.0.

[Output Format]
You MUST return ONLY a valid JSON object. Do not output markdown code blocks like ```json.
The JSON must have exactly these two keys:
- "thought": A brief string explaining your step-by-step analysis.
- "score": A float number between 0.0 and 1.0.
"""
        input_str = "- Original Schema:\n" + db_schema + "\n- Original User Query:\n" + original_query + '\n- Masked Query:\n' + masked_query
        try:
            # 调用大模型进行打分 (为了速度和成本，建议这里的 temperature 设为 0)
            response = client.chat.completions.create(
                model=model_name, # 如果有更便宜/快的模型(如 deepseek-chat)，也可以在这里硬编码替换
                    messages=[
                    {"role": "system", "content": judge_prompt},
                    {"role": "user", "content": example_user1},
                    {"role": "assistant", "content": example_assist1},
                    {"role": "user", "content": example_user2},
                    {"role": "assistant", "content": example_assist2},
                    # 放入当前待处理的问题
                    {"role": "user", "content": input_str}
                ],
                temperature=0.0,
                # 如果使用的模型支持强制 JSON 模式，可以加上下面这行；不支持则注释掉
                response_format={"type": "json_object"} 
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # 解析 JSON 结果
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