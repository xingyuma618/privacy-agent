system_prompt = '''你是一个数据库技术专家，我将提供给你数据库schema，用户的自然语言问题，请你生成对应的SQL语句，由于本地数据具备隐私性，因此我提供的是掩码后的自然语言查询和schema。请将你生成的SQL语句放在```sql\n\n```标签中。
'''
def get_sql_generate_template(input_str):
    messages=[
        {"role": "system", "content": system_prompt},

        # 放入当前待处理的问题
        {"role": "user", "content": input_str}
    ]
    return messages