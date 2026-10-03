
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))

import json
import os
import re
import time
import sqlite3
import subprocess
import requests
from openai import OpenAI
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from func_timeout import func_timeout, FunctionTimedOut
from functools import partial

# ================= 1. 全局配置区域 =================

# --- NL2SQL 基础配置 (固定不变的模型) ---
SYSTEM_PROMPT_NL2SQL = '''You are an expert on the SQLite database. I will give you the database schema and the user's question. Your job is to answer the question by generating a SQL statement. 
Please enclose SQL statements within ```sql\n...\n``` tags.'''
NL2SQL_MODEL = '/mnt/wh_intern/MXY/models/Qwen2.5-Coder-14B-Instruct'
NL2SQL_API_BASE = "http://127.0.0.1:2338/v1"

# --- Query 改写基础配置 (动态切换的模型) ---
SYSTEM_PROMPT_QUERY_REWRITE = "这是一个query改写任务，我将提供给你用户的原始query，你需要将其转换为更通俗易懂的query"
REWRITE_PORT = 2335
REWRITE_API_BASE = f"http://127.0.0.1:{REWRITE_PORT}/v1"

# --- 【重点】待测试的 Checkpoint 列表 ---
# 把你所有需要测试的 checkpoint 绝对路径加到这个列表里
CHECKPOINTS = [
    str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-25'),
    str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-50'),
    str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-75'),
    str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-100'),
    str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-125'),

    str(_RELEASE_ROOT / 'LlamaFactory/saves/query_rewrite_v2/checkpoint-150'),
]

# --- 数据与并发配置 ---
DATA_PATH = "/mnt/wh_intern/MXY/paper/rl/ours/verl/data/oiginal_data/eval_data/bird_chess_sc_dev.json"
DB_BASE_DIR = str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1')
MAX_WORKERS = 16 

# ================= 2. 核心功能函数 =================

def extract_code(text, language='sql'):
    """提取大模型输出的 SQL 语句"""
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
    """连接数据库并对比执行结果"""
    conn = sqlite3.connect(db_path)
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

def wait_for_server(url, timeout=300):
    """轮询检测模型 API 服务是否已完全启动"""
    start_time = time.time()
    print(f"正在等待 LMDeploy 服务就绪 ({url}) ...")
    while time.time() - start_time < timeout:
        try:
            # 请求模型的 models 接口检查状态
            response = requests.get(f"{url}/models")
            if response.status_code == 200:
                print("服务已就绪！可以开始评测。")
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)
    print("等待服务就绪超时！")
    return False

def process_item(item, rewrite_model_name, rewrite_client, nl2sql_client):
    """单条数据处理逻辑（运行在子线程中）"""
    db_schema = item.get('db_schema', '')
    question = item.get('question', '')
    evidence = item.get('evidence', '')
    
    evidence_str = f" {evidence}" if evidence else ""
    query_rewrite_input_str = (
        f"- Database Schema:\n{db_schema}\n"
        f"- User's Question:\n{question}{evidence_str}"
    )
    
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT_QUERY_REWRITE},
        {"role": "user", "content": query_rewrite_input_str}
    ]
    
    try:
        # 1. 调用 LMDeploy 改写大模型
        rewrite_response = rewrite_client.chat.completions.create(
            model=rewrite_model_name,
            messages=messages,
            temperature=0.0,
            max_tokens=4096
        )
        generate_question = rewrite_response.choices[0].message.content
        
        # 2. 调用固定的 NL2SQL Coder 模型
        nl2sql_input_str = "- Database Schema:\n" + db_schema + "\n- User's question:\n" + generate_question
        nl2sql_response = nl2sql_client.chat.completions.create(
            model=NL2SQL_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT_NL2SQL},
                {"role": "user", "content": nl2sql_input_str}
            ],
            temperature=0.0,
            max_tokens=4096
        )
        pred_sql = extract_code(nl2sql_response.choices[0].message.content)
        db_path = f"{DB_BASE_DIR}/{item['db_id']}/{item['db_id']}.sqlite"
        
        # 3. 设定 20 秒超时执行 SQL 对比
        acc = func_timeout(20.0, execute_sql, args=(pred_sql, item['gold_sql'], db_path))
        if acc == 1:   
            result_item = item.copy()
            # 记录中间结果，方便排查和复盘
            result_item['generated_question'] = generate_question
            result_item['predicted_sql'] = pred_sql
            return result_item
            
    except FunctionTimedOut:
        return None
    except Exception as e:
        # 捕获请求错误，防止中断整体评测
        return None
        
    return None

def evaluate_checkpoint(ckpt_path, data, nl2sql_client):
    """启动 LMDeploy 服务 -> 评测 -> 关闭服务的生命周期管理"""
    ckpt_name = os.path.basename(ckpt_path.rstrip('/'))
    print(f"\n{'='*70}")
    print(f"🚀 正在准备测试 Checkpoint: {ckpt_name}")
    print(f"{'='*70}")
    
    # 设置显卡环境变量，固定在卡 6
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"] = "6"  
    
    # 构造 LMDeploy 启动命令
    cmd = [
        "lmdeploy", "serve", "api_server",
        "--log-level=INFO",
        "--server-port", str(REWRITE_PORT),
        "--tp=1",
        ckpt_path
    ]
    
    print(f"执行启动命令: CUDA_VISIBLE_DEVICES=6 {' '.join(cmd)}")
    
    # 启动子进程，输出重定向避免刷屏
    server_process = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    try:
        # 等待服务就绪
        if not wait_for_server(REWRITE_API_BASE):
            print(f"⚠️ 跳过 {ckpt_name}: 服务启动失败或超时。")
            return
        
        # 初始化当前 Checkpoint 的 OpenAI 客户端
        rewrite_client = OpenAI(api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"), base_url=REWRITE_API_BASE)
        processed_data = []
        
        # 启动线程池并发评测
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # partial 绑定固定参数
            task = partial(
                process_item, 
                rewrite_model_name=ckpt_path, # LMDeploy 默认模型名为路径
                rewrite_client=rewrite_client, 
                nl2sql_client=nl2sql_client
            )
            futures = [executor.submit(task, item) for item in data]
            
            # 进度条显示
            for future in tqdm(as_completed(futures), total=len(data), desc=f"Evaluating {ckpt_name}"):
                try:
                    result = future.result()
                    if result:
                        processed_data.append(result)
                except Exception as e:
                    print(f"\n任务抛出异常: {e}")
        
        # 保存结果到独立文件
        output_path = f"./rewrite_{ckpt_name}.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)
            
        print(f"✅ [{ckpt_name}] 处理完成！答对 (Acc=1) 数量: {len(processed_data)}，结果已保存至 {output_path}")
        
    finally:
        # 无论成功还是异常，确保杀死 LMDeploy 进程并释放显存
        print(f"🛑 正在关闭 {ckpt_name} 的 LMDeploy 进程并释放显卡 6 的显存...")
        server_process.terminate()
        server_process.wait()
        time.sleep(5)  # 等待系统完全回收显存

# ================= 3. 主程序入口 =================

if __name__ == "__main__":
    # 1. 提前加载测试集数据
    with open(DATA_PATH, 'r', encoding='utf-8') as f:
        eval_data = json.load(f)
    
    # 2. 初始化全局唯一的 NL2SQL 客户端
    global_nl2sql_client = OpenAI(api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"), base_url=NL2SQL_API_BASE)
    
    print(f"共发现 {len(CHECKPOINTS)} 个待测 Checkpoint。")
    print("注意: 确保固定的 NL2SQL 进程(端口2338)已在其他显卡或窗口启动完毕！")
    
    # 3. 循环遍历所有 Checkpoint，自动按序评测
    for ckpt in CHECKPOINTS:
        evaluate_checkpoint(ckpt, eval_data, global_nl2sql_client)
        
    print("\n🎉 所有 Checkpoint 自动化测试任务执行完毕！")




    # eval_cmd = f'''python bird_eval/evaluation.py \
    #     --db_root_path {db_base_dir} \
    #     --predicted_sql_path {predicted_sql_path} \
    #     --data_mode {data_mode} \
    #     --ground_truth_path {ground_truth_path} \
    #     --num_cpus {num_cpus} \
    #     --mode_gt {mode_gt} \
    #     --mode_predict {mode_predict} \
    #     --diff_json_path {diff_json_path} \
    #     --meta_time_out {meta_time_out} \
    #     --output_dir {output_dir}
    # ''' 
    # subprocess.run(eval_cmd, shell=True, check=True)

# 966
#  ckpt 25 ,862
# ckpt 50 867. 56.51
# ckpt 75

# 888. 57.89

#  ckpt 25 ,899
# ckpt 50 899
# ckpt 75


#  ckpt 25 ,906
# ckpt 50 901
# ckpt 75


# ckpt 50 914
# ckpt 75