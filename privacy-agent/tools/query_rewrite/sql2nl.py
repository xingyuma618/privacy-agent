import json
import os
import hashlib
import threading
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # 用于显示进度条
from sql2nl_template import get_sql2nl_messages

# --------------------------------------------------------

def get_unique_id(item):
    """
    根据数据内容生成唯一哈希指纹，用于判断数据是否已处理
    """
    content = str(item.get('db_schema', '')) + str(item.get('gold_sql', ''))
    return hashlib.md5(content.encode('utf-8')).hexdigest()


def process_single_item(item):
    """
    处理单条数据的函数
    """
    # client = OpenAI(
    #     api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),  
    #     base_url="http://127.0.0.1:2333/v1"  
    # ) 
    # model_name = '/root/models/Qwen3-235B-A22B-Instruct' #改写后做对了6781条

    model_name = os.getenv("SYNTHESIS_MODEL_NAME", "Qwen3.6-27B")
    client = OpenAI(
        api_key=os.getenv("SYNTHESIS_API_KEY", "EMPTY"),
        base_url=os.getenv("SYNTHESIS_BASE_URL", "http://127.0.0.1:8039/v1")
    )
    
    input_str = "- Database Schema:\n" + str(item.get('db_schema', '')) +"\n- User's Question:\n" + item['question'] + ' ' +item['evidence'] + '\n- SQL:\n' + str(item.get('gold_sql', ''))
    
    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=get_sql2nl_messages(input_str),
            temperature=0.0,
            max_tokens=4096
        )
        
        generate_question = response.choices[0].message.content
        print(generate_question)
        item['generate_question'] = generate_question
        
    except Exception as e:
        item['generate_question'] = f"ERROR: {str(e)}"
        
    return item

if __name__ == '__main__':
    # 1. 路径设置
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.getenv("SQL2NL_DATA_PATH", os.path.join(script_dir, "bird.json"))
    output_json_path = os.getenv("SQL2NL_OUTPUT_JSON", os.path.join(script_dir, "bird_sql2nl.json"))
    # # 中间文件：使用 jsonl 格式进行追加存储
    output_jsonl_path = os.getenv("SQL2NL_OUTPUT_JSONL", os.path.join(script_dir, "bird_sql2nl_ckpt.jsonl")) 

    # data_path = os.path.join(script_dir, "spider.json")
    # output_json_path = "./spider_1.json"
    # 中间文件：使用 jsonl 格式进行追加存储
    # output_jsonl_path = "./spider_checkpoint_1.jsonl" 

    # 2. 加载全量原始数据
    with open(data_path, 'r', encoding='utf-8') as f:
        all_data = json.load(f)

    # 3. 读取已处理的数据指纹，实现断点续传
    processed_ids = set()
    if os.path.exists(output_jsonl_path):
        print(f"发现历史断点文件 {output_jsonl_path}，正在加载历史记录...")
        with open(output_jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                    processed_ids.add(get_unique_id(record))
                except json.JSONDecodeError:
                    continue # 忽略损坏的单行

    # 4. 过滤出还需要处理的数据
    pending_data = [item for item in all_data if get_unique_id(item) not in processed_ids]
    
    print(f"总数据量: {len(all_data)}")
    print(f"已处理量: {len(processed_ids)}")
    print(f"本次待处理: {len(pending_data)}")

    if len(pending_data) > 0:
        # 5. 设置并发与文件锁
        MAX_WORKERS = 1
        write_lock = threading.Lock() # 用于保证多线程写文件时不会互相覆盖

        print(f"\n开始处理，并发线程: {MAX_WORKERS}")
        
        # 以追加模式('a')打开 jsonl 文件
        with open(output_jsonl_path, 'a', encoding='utf-8') as out_f:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # 提交未处理的任务
                futures = [executor.submit(process_single_item, item) for item in pending_data]
                
                for future in tqdm(as_completed(futures), total=len(pending_data), desc="正在处理 SQL 转自然语言"):
                    try:
                        result = future.result()
                        # 获取到结果后，立即加上线程锁写入文件
                        with write_lock:
                            # 存入一条 json 并换行
                            out_f.write(json.dumps(result, ensure_ascii=False) + '\n')
                            out_f.flush() # 核心：强制立即刷入磁盘，避免留在内存缓冲区
                    except Exception as e:
                        print(f"\n任务执行过程中出现未捕获异常: {e}")
    else:
        print("所有数据均已处理完毕，无需重复请求。")

    # 6. 全部处理完成后，将 jsonl 转换为最终的结构化 json 数组
    print("\n正在将断点文件转换为最终的 JSON 格式...")
    final_data = []
    with open(output_jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                final_data.append(json.loads(line.strip()))

    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
    
    print(f"✅ 处理完成！最终结果已保存至 {output_json_path}")
