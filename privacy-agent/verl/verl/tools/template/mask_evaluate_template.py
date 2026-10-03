system_prompt = '''我将提供给你数据库原始schema，用户的原始问题，掩码后的schema和掩码后的问题，你需要评估掩码后的用户问题，是否存在以下问题。
1. 实体漏掩码
2. 掩码后的用户问题与原始的用户问题存在较大差异或结构不完整等问题，导致query无法理解要查询什么内容。

'''

def get_mask_evaluate_template(input_str):
    messages=[
            {"role": "system", "content": system_prompt},

            {"role": "user", "content": input_str}
    ]
    return messages

