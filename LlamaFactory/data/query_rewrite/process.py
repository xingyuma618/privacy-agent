import json
import os

# 配置文件路径
data = json.load(open(str(__import__("pathlib").Path(__import__("os").environ.get("PRIVACY_AGENT_RELEASE_ROOT", __import__("pathlib").Path(__file__).resolve().parents[3])) / 'privacy-agent/tools/semantic_compare/spider-1.json'), 'r')) + json.load(open(str(__import__("pathlib").Path(__import__("os").environ.get("PRIVACY_AGENT_RELEASE_ROOT", __import__("pathlib").Path(__file__).resolve().parents[3])) / 'privacy-agent/tools/semantic_compare/bird-1.json'), 'r'))

output_file = str(__import__("pathlib").Path(__import__("os").environ.get("PRIVACY_AGENT_RELEASE_ROOT", __import__("pathlib").Path(__file__).resolve().parents[3])) / 'LlamaFactory/data/query_rewrite/v2/query_rewrite_train.json')

def process_spider_data():
    # 读取原始数据

    processed_data = []
    
    for item in data:
        # 提取字段（使用 .get() 防止某些字段缺失报错）
        db_schema = item.get('db_schema', '')
        question = item.get('question', '')
        evidence = item.get('evidence', '')
        generate_question = item.get('generate_question', '')
        
        # 处理 evidence 为空的情况，避免多出多余的空格
        evidence_str = f" {evidence}" if evidence else ""
        
        # 拼接 user 字段内容
        user_content = (
            f"- Database Schema:\n{db_schema}\n"
            f"- User's Question:\n{question}{evidence_str}"
        )
        
        # 组装为 LLaMA-Factory 兼容的 messages 格式
        processed_item = {
            "messages": [
                {
                    "role": "user",
                    "content": user_content
                },
                {
                    "role": "assistant",
                    "content": generate_question
                }
            ]
        }
        processed_data.append(processed_item)

    # 保存处理后的数据
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, ensure_ascii=False, indent=2)

    print(f"数据处理完成！共处理了 {len(processed_data)} 条数据。")
    print(f"处理后的文件已保存至: {output_file}")

if __name__ == "__main__":
    process_spider_data()