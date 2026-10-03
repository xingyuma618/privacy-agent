import argparse
import os
import datasets

# 设置环境变量，使用国内镜像站
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

# ==========================================
# 填入你之前的完整 System Prompt
# ==========================================
SYSTEM_PROMPT = """
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

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    verl_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    package_root = os.path.abspath(os.path.join(verl_root, "..", ".."))
    privacy_agent_root = os.path.join(package_root, "privacy-agent")

    parser = argparse.ArgumentParser()
    # 默认指向你的 jsonl 数据文件
    parser.add_argument("--local_dataset_path", default=os.path.join(privacy_agent_root, "agent_data_create", "data", "bird_train_sc_filtered.jsonl"), help="The local path to the raw JSONL dataset.")
    # 默认输出的 parquet 目录
    parser.add_argument("--local_save_dir", default=os.path.join(verl_root, "data", "privacy-agent"), help="The save directory for the preprocessed dataset.")
    # 数据库的基础路径（用于构造 db_path）
    parser.add_argument("--db_base_dir", default=os.getenv("BIRD_DB_BASE_DIR", os.path.join(privacy_agent_root, "CHESS", "data", "train", "train_databases_1")), help="Base directory for sqlite databases.")
    
    args = parser.parse_args()

    # 1. 使用 datasets 直接加载 JSONL 文件
    dataset = datasets.load_dataset("json", data_files={"train": args.local_dataset_path})
    
    # 假设你的数据只有 train，为了满足 verl 需求，我们可以切分一点点作为 test，或者干脆全部作为 train
    # 这里我们把 95% 作为训练集，5% 作为测试验证集
    dataset = dataset["train"].train_test_split(test_size=0.05, seed=42)
    train_dataset = dataset["train"]
    test_dataset = dataset["test"]

    # 2. 定义映射函数，将原始数据转化为 verl Agent 需要的格式
    def make_map_fn(split):
        def process_fn(example, idx):
            # 提取原始字段
            db_id = example.get("db_id", "")
            question = example.get("question", "")
            evidence = example.get("evidence", "")
            gold_sql = example.get("gold_sql", "")
            db_schema = example.get("db_schema", "")
            structured_db_schema = example.get("structured_db_schema", {})

            # 构建 User Prompt (与你之前的代码保持一致)
            user_prompt = f"- Database Schema:\n{db_schema}\n\n- User Question:\n{question} {evidence}\n\nTask started. Please provide your analysis (Thought) and call the tools (Action)."

            # 构建当前 case 的真实数据库路径
            db_path = f"{args.db_base_dir}/{db_id}/{db_id}.sqlite"

            # 组装 verl 需要的数据结构
            data = {
                "data_source": "bird_privacy_agent",
                "agent_name": "tool_agent",
                "prompt": [
                    {
                        "role": "system",
                        "content": SYSTEM_PROMPT.strip(),
                    },
                    {
                        "role": "user",
                        "content": user_prompt,
                    },
                ],
                "ability": "nl2sql",
                "reward_model": {
                    "style": "rule", 
                    "ground_truth": gold_sql
                },
                "extra_info": {
                    "split": split,
                    "index": idx,
                    "db_id": db_id,
                    'db_schema': db_schema,
                    'structured_db_schema': structured_db_schema,
                    'question': question,
                    'evidence': evidence,
                    "gold_sql": gold_sql,
                    # =========================================================
                    # 核心机制：为工具注入 kwargs
                    # 这些参数会在 Tool 执行时，通过 **kwargs 传递给工具的 execute 方法
                    # =========================================================
                    "need_tools_kwargs": True,
                    "tools_kwargs": {
                        "query_rewrite": {
                            "execute_kwargs": {"db_schema": db_schema}
                        },
                        "entity_extract": {
                            "execute_kwargs": {"db_schema": db_schema}
                        },
                        "apply_typed_masking": {
                            "execute_kwargs": {"structured_db_schema": structured_db_schema}
                        },
                        "execute_sql_check": {
                            "execute_kwargs": {"db_path": db_path}
                        }
                    },
                },
            }
            return data

        return process_fn

    # 3. 应用 Map 函数
    print("Processing train dataset...")
    train_dataset = train_dataset.map(function=make_map_fn("train"), with_indices=True, remove_columns=train_dataset.column_names)
    
    print("Processing test dataset...")
    test_dataset = test_dataset.map(function=make_map_fn("test"), with_indices=True, remove_columns=test_dataset.column_names)

    # 4. 创建保存目录并保存为 parquet
    os.makedirs(args.local_save_dir, exist_ok=True)
    train_dataset.to_parquet(os.path.join(args.local_save_dir, "train.parquet"))
    test_dataset.to_parquet(os.path.join(args.local_save_dir, "test.parquet"))

    print(f"✅ Data processing complete! Saved to {args.local_save_dir}")
    print(f"Train samples: {len(train_dataset)}, Test samples: {len(test_dataset)}")
