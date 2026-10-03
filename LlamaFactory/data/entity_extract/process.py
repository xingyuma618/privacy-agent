import json
import os

def merge_and_convert_to_messages_format(input_files, output_file):
    combined_data = []
    
    for file_path in input_files:
        if not os.path.exists(file_path):
            print(f"警告: 文件未找到 {file_path}")
            continue
            
        print(f"正在处理: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                
                try:
                    data = json.loads(line)
                    
                    # 提取所需的字段
                    db_schema = data.get("db_schema", "")
                    generate_question = data.get("generate_question", "")
                    extracted_entities = data.get("extracted_entities", [])
                    
                    # 构造 User 侧的数据
                    user_content = f"- Database Schema:\n{db_schema}\n- User's Question:\n{generate_question}"
                    
                    # 构造 Assistant 侧的数据 (由于实体是列表，转换为 JSON 字符串)
                    assistant_content = json.dumps(extracted_entities, ensure_ascii=False)
                    
                    # 组合成 messages 格式
                    entry = {
                        "messages": [
                            {
                                "role": "user",
                                "content": user_content
                            },
                            {
                                "role": "assistant",
                                "content": assistant_content
                            }
                        ]
                    }
                    
                    combined_data.append(entry)
                except Exception as e:
                    print(f"解析行出错: {e}")

    # 将合并后的所有数据存入一个新的 JSON 文件
    with open(output_file, 'w', encoding='utf-8') as out_f:
        json.dump(combined_data, out_f, ensure_ascii=False, indent=2)
    
    print(f"处理完成！共处理 {len(combined_data)} 条数据。")
    print(f"输出文件已保存至: {output_file}")

# 定义输入文件路径 (包含 bird 和 spider 两个文件)
input_paths = [
    str(__import__("pathlib").Path(__import__("os").environ.get("PRIVACY_AGENT_RELEASE_ROOT", __import__("pathlib").Path(__file__).resolve().parents[3])) / 'privacy-agent/tools/schema_linking/bird_sc_extracted.jsonl'),
    str(__import__("pathlib").Path(__import__("os").environ.get("PRIVACY_AGENT_RELEASE_ROOT", __import__("pathlib").Path(__file__).resolve().parents[3])) / 'privacy-agent/tools/schema_linking/spider_sc_extracted.jsonl')
]

# 定义输出文件路径 (可以根据你的需要修改)
output_path = str(__import__("pathlib").Path(__import__("os").environ.get("PRIVACY_AGENT_RELEASE_ROOT", __import__("pathlib").Path(__file__).resolve().parents[3])) / 'LlamaFactory/data/entity_extract/merged_schema_linking_sharegpt.json')

# 执行转换
merge_and_convert_to_messages_format(input_paths, output_path)