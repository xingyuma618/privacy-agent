# ---------------- 提示词定义部分保持不变 ----------------
# system_prompt = '''# Role
# 你是一个专业的数据库查询分析师。你的任务是将用户的自然语言问题（User's Question）改写为更直白的查询表述，以便后续进行高精度的信息提取。

# # Constraints
# 1. 改写后的表述必须与原始问题语义完全一致，不得遗漏、增加或篡改用户的查询意图。
# 2. 改写后的query需要直观的看出来设query中涉及哪些列名、表名、cell value等数据库实体。
# 3. 如果用户提供了【Rewrite Instruction】，说明上一次的改写存在问题。你必须仔细阅读该说明，并在本次改写中严格遵守这些纠错要求。
# '''

# example_user1 = '''- Database Schema:
# CREATE TABLE head\n(\n\t\"age\" real, -- Example Values: `67.0`, `68.0`, `69.0`, `52.0`, `53.0`   \n\t\"head_ID\" int, --  \n\tPRIMARY KEY (\"head_ID\"),\n);
# - User's Question:
# How many heads of the departments are older than 56 ?
# '''

# example_assist1 = '''Count the number of heads where the age is greater than 56.'''

# example_user2 = '''- Database Schema:
# CREATE TABLE movies\n(\n\tmovie_id INTEGER not null primary key,\n\tdirector_name TEXT, -- Example Values: `Ren Blood`   \n\tmovie_title TEXT, -- Example Values: `('La Antena',)`, `('Elementary Particles',)`, `(\"It's Winter\",)`  | Column Description: Name of the movie \n);\n\n
# - User's Question:
# Who is the director of the movie Sex, Drink and Bloodshed? Sex, Drink and Bloodshed refers to movie title = 'Sex, Drink and Bloodshed';
# '''

# example_assist2 = '''Find the director name of the movie titled 'Sex, Drink and Bloodshed'.'''


# def get_query_rewrite_messages(input_str):
#     messages=[
#                 {"role": "system", "content": system_prompt},
#                 {"role": "user", "content": example_user1},
#                 {"role": "assistant", "content": example_assist1},
#                 {"role": "user", "content": example_user2},
#                 {"role": "assistant", "content": example_assist2},
#                 {"role": "user", "content": input_str}
#             ]
#     return messages







system_prompt ="""这是一个query改写任务，我将提供给你用户的原始query，你需要将其转换为更通俗易懂的query。
"""
def get_query_rewrite_messages(input_str):
    messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": input_str}
            ]
    return messages