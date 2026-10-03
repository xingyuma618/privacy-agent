import json
import os  # 新增：用于检查文件是否存在
from openai import OpenAI  
import json_repair
import concurrent.futures
from tqdm import tqdm  
from schema_linking_template import get_schema_linking_template
from revise_template import get_schema_linking_revise_template


def read_jsonl_to_list(path, encoding="utf-8"):
    # 增加文件存在性检查，如果文件不存在则返回空列表
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding=encoding) as f:
        return [json.loads(line) for line in f if line.strip()]
    

# ----------------- 抽离出单次请求的处理函数 -----------------
def process_single_item(item, client, model_name):
    input_str = "- Database Schema:\n" + item['db_schema'] + "\n- User's question:\n" + item['generate_question'] + '\n- Reference SQL:\n' + item['gold_sql']
    
    response = client.chat.completions.create(
        model=model_name,
        messages=get_schema_linking_template(input_str),
        max_tokens=4096,
        extra_body={"thinking": {"type": "disabled"}},
        stream=False
    )
    
    extracted_entities = response.choices[0].message.content

    item['extracted_entities'] = json_repair.loads(extracted_entities)
    # 建议在多线程跑大数据量时，把下面这几个 print 注释掉，避免控制台输出太乱
    # print('===============================================')
    print(input_str)
    print(extracted_entities)
    return item
        

# ----------------- 获取唯一标识符（用于去重） -----------------
def get_unique_key(item):
    """
    提取数据的唯一标识，用于判断某条数据是否已经处理过。
    优先使用 question_id，如果没有则使用 generate_question
    如果你的数据里有更合适的唯一ID，可以修改这里。
    """
    return item.get('question_id', item.get('generate_question', str(item)))


if __name__=='__main__':
    # 1. 读取全量原始数据
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tools_root = os.path.dirname(script_dir)
    data_path = os.getenv("SCHEMA_LINKING_DATA_PATH", os.path.join(tools_root, "semantic_compare", "bird-1.json"))
    output_path = os.getenv("SCHEMA_LINKING_OUTPUT_PATH", os.path.join(script_dir, "bird_sc_extracted_deepseek_v4.jsonl"))

    # data_path = os.path.join(tools_root, "semantic_compare", "spider-1.json")
    # output_path = os.path.join(script_dir, "spider_sc_extracted.jsonl")


    data = json.load(open(data_path, 'r'))

    # client = OpenAI(
    #     api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),  # 本地部署的服务通常不需要验证 API Key，填 "EMPTY" 或任意字符串即可
    #     base_url="http://127.0.0.1:2333/v1"  # 注意：OpenAI SDK 通常要求加上 /v1 后缀
    # )
    # model_name = '/root/models/Qwen3-235B-A22B-Instruct'
    client = OpenAI(
        api_key=os.getenv("SCHEMA_LINKING_API_KEY", "EMPTY"),
        base_url=os.getenv("SCHEMA_LINKING_BASE_URL", "http://127.0.0.1:8039/v1")
    )
    model_name = os.getenv("SCHEMA_LINKING_MODEL_NAME", "Qwen3.6-27B")
    # client = OpenAI(
    #     api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),
    #     base_url="https://open.bigmodel.cn/api/paas/v4" 
    # )

    # model_name = 'gemini-3-pro'
    # client = OpenAI(
    #     api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),  # 本地部署的服务通常不需要验证 API Key，填 "EMPTY" 或任意字符串即可
    #     base_url="https://127api.top/v1"  # 注意：OpenAI SDK 通常要求加上 /v1 后缀
    # )

    # model_name = 'kimi-k2-0905-preview'
    # client = OpenAI(
    #     api_key=__import__("os").environ.get("OPENAI_API_KEY", "EMPTY"),  
    #     base_url="https://api.moonshot.cn/v1"  
    # )

    
    # ================= 核心修改：断点续存逻辑 =================
    
    # 2. 读取已经处理过的数据（如果文件存在的话）
    processed_data = read_jsonl_to_list(output_path)
    
    # 3. 提取已经处理过的数据的唯一标识集合
    # 这样查找速度是 O(1)
    processed_keys = set(get_unique_key(item) for item in processed_data if item.get('extracted_entities') != "ERROR")
    
    # 4. 过滤出还需要处理的数据 (尚未处理，或者之前处理报错的)
    pending_data = [item for item in data if get_unique_key(item) not in processed_keys]
    
    # =========================================================

    MAX_WORKERS = 1

    print(f"原始数据总量: {len(data)}")
    print(f"已完成数量: {len(processed_keys)}")
    print(f"本次待处理数量: {len(pending_data)}")
    print(f"并发线程数: {MAX_WORKERS}")

    if len(pending_data) == 0:
        print("所有数据已处理完毕！")
        exit()

    # 5. 打开文件准备**实时追加** (注意这里是 "a" 模式，append)
    with open(output_path, "a", encoding="utf-8") as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交待处理的任务
            futures = [executor.submit(process_single_item, item, client, model_name) for item in pending_data]
            
            # as_completed 会在主线程中依次 yield 完成的任务
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="API Requests"):
                result_item = future.result()
                
                # 6. 每完成一条，立刻写入文件并刷新缓冲区
                f.write(json.dumps(result_item, ensure_ascii=False) + "\n")
                f.flush()  # 关键：强制将内存中的数据写入硬盘，防止突然中断丢失

    print(f"\n处理完成，所有结果已更新至: {output_path}")
