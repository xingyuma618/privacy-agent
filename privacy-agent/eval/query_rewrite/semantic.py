
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))

import json
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # 用于显示进度条
import re
import sqlite3
from func_timeout import func_timeout, FunctionTimedOut

# ---------------- 提示词定义部分保持不变 ----------------
system_prompt_zh = '''You are an expert on the SQLite database. I will give you the database schema and the user's question. Your job is to answer the question by generating a SQL statement. 
Please enclose SQL statements within ```sql\n...\n``` tags.'''


# --------------------------------------------------------
def extract_code(text, language='sql'):
    sql = text
    if '```sql' in text:
        pattern = r"```sql(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        if len(matches) > 0:
            sql = matches[-1].replace('\n', ' ').strip()
            return sql
    sql = sql.replace('\n', ' ').strip()
    return sql

def execute_sql(predicted_sql, ground_truth, db_path):
    conn = sqlite3.connect(db_path)
    # Connect to the database
    cursor = conn.cursor()
    predicted_sql = predicted_sql.strip().rstrip(';')
    ground_truth = ground_truth.strip().rstrip(';')
    cursor.execute(predicted_sql)
    predicted_res = cursor.fetchall()
    cursor.execute(ground_truth)
    ground_truth_res = cursor.fetchall()
    res = 0
    if set(predicted_res) == set(ground_truth_res):
        res = 1
    return res

# 将核心逻辑封装为一个独立函数，方便并发调用
def process_single_item(item):
    """
    处理单条数据的函数
    """
    # 在每个工作线程中初始化 client（避免跨线程/进程共享引发的潜在连接池问题）
    client = OpenAI(
        api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),  
        base_url="http://127.0.0.1:2335/v1"  
    )
    model_name = '/mnt/wh_intern/MXY/models/Qwen2.5-Coder-14B-Instruct'
    
    input_str = "- Database Schema:\n" + item['db_schema'] + "\n- User's question:\n" + item['question']  + ' ' +item['evidence']
    # input_str = "- Database Schema:\n" + item['db_schema'] + "\n- User's question:\n" + item['generate_question'] 


    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system_prompt_zh},
            {"role": "user", "content": input_str}
        ],
        temperature=0.0,
        max_tokens=4096
    )
    # print(response.choices[0].message.content)
    pred_sql = extract_code(response.choices[0].message.content)
    db_path = f"{db_base_dir}/{item['db_id']}/{item['db_id']}.sqlite"
    # print(db_path)
    try:
        acc = func_timeout(20.0, execute_sql,
                                args=(pred_sql, item['gold_sql'], db_path))
        if acc ==1:   
            return item
        
    except FunctionTimedOut:
        return None
    
    return None

if __name__ == '__main__':
    # 1. 加载数据
    # bird
    data_path =  "/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/bird_chess_sc_dev.json"
    db_base_dir = str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1')
    output_path = "./bird-1.json"
    
    # spider
    # data_path = "/mnt/wh_intern/MXY/paper/privacy-agent/tools/spider.json"
    # db_base_dir = "/root/data/nl2sql/spider/database"
    # output_path = "./spider-1.json"

    # data_path = "/mnt/wh_intern/MXY/paper/rl/noise_aware/verl/data/original_data/spider/v0_sc/data/train.json"

    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    processed_data = []
    
    # 2. 设置并发数
    MAX_WORKERS = 32

    # 3. 使用 ThreadPoolExecutor + tqdm
    print(f"开始处理，总数: {len(data)}, 并发线程: {MAX_WORKERS}")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 第一步：提交所有任务，获取 Future 对象列表
        futures = [executor.submit(process_single_item, item) for item in data]
        
        # 第二步：使用 tqdm 包裹 as_completed 迭代器
        # total=len(data) 告诉 tqdm 总量是多少，这样它能计算百分比和预估剩余时间
        for future in tqdm(as_completed(futures), total=len(data), desc="正在处理 SQL 转自然语言"):
            try:
                result = future.result()
                if result:
                    processed_data.append(result)
            except Exception as e:
                print(f"\n任务执行过程中出现未捕获异常: {e}")
    print(len(processed_data))
    # 4. 保存结果
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=4)
    
    print(f"\n✅ 处理完成！结果已保存至 {output_path}")


