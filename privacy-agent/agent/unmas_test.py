import re
import json

def unmask_sql(masked_text: str, unmask_dict: dict) -> str:
    """
    根据反向映射字典 (unmask_dict) 将掩码后的 SQL 或 Query 还原为真实内容。
    
    Args:
        masked_text (str): 掩码后的 SQL 语句或自然语言 Query。
        unmask_dict (dict): _tool_apply_typed_masking 生成的反向映射字典。
        
    Returns:
        str: 还原后的原始文本。
    """
    if not masked_text or not isinstance(masked_text, str):
        return masked_text

    # 1. 将分层的字典 (tables, columns, values) 展平为一个一维的映射字典
    flat_map = {}
    for category in ["tables", "columns", "values"]:
        if category in unmask_dict:
            flat_map.update(unmask_dict[category])

    # 2. 按掩码键的长度降序排序 (例如优先处理 val_10，再处理 val_1)
    sorted_masks = sorted(flat_map.keys(), key=len, reverse=True)

    # 3. 逐个执行替换
    unmasked_text = masked_text
    for mask in sorted_masks:
        original_value = str(flat_map[mask])
        
        # 使用正则表达式的 \b (单词边界) 来防止子串错误匹配
        # 例如: \bcol_1\b 只会匹配 "col_1" 本身，而不会匹配 "col_10" 中的前半部分
        pattern = re.compile(r'\b' + re.escape(mask) + r'\b', re.IGNORECASE)
        
        # 使用 lambda 函数返回 original_value，避免如果原始值里包含特殊的正则转义符（如 \1）导致报错
        unmasked_text = pattern.sub(lambda _: original_value, unmasked_text)

    return unmasked_text

# ==========================================
# 测试代码执行部分
# ==========================================
if __name__ == "__main__":
    # 假设这是你用大语言模型生成的掩码版 SQL
    masked_generated_sql = "SELECT table_1.col_3, table_1.col_5 FROM table_1 WHERE table_1.col_3 < val_1 AND table_1.col_5 = 'val_9';"

    # 之前掩码操作生成的字典
    unmask_dict = {
        "tables": {
            "table_1": "satscores"
        },
        "columns": {
            "col_1": "cds",
            "col_2": "AvgScrWrite",
            "col_3": "AvgScrMath",
            "col_4": "AvgScrRead",
            "col_5": "cname"
        },
        "values": {
            "val_1": "417",
            "val_2": "505",
            "val_3": "395",
            "val_4": "418",
            "val_5": "546",
            "val_6": "387",
            "val_7": "503",
            "val_8": "397",
            "val_9": "Calaveras"
        }
    }

    # 执行还原
    restored_sql = unmask_sql(masked_generated_sql, unmask_dict)

    print("=== LLM 生成的掩码 SQL ===")
    print(masked_generated_sql, "\n")

    print("=== 还原后的真实 SQL ===")
    print(restored_sql)