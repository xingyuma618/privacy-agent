# ---------------- 提示词定义部分保持不变 ----------------
# system_prompt_en = '''This is a SQL-to-natural-language query task. I will provide a database schema and SQL query statements. Please write the corresponding natural-language query. The natural-language query should be very intuitive and easy to understand. In other words, it should mention the column names, table names, and cell values that appear in the SQL.
# '''
# system_prompt_zh = '''
# This is an SQL-to-Natural-Language task. I will provide you with a database schema and an SQL statement. Your task is to generate a natural language query for the given SQL statement. The natural language query you generate must explicitly and intuitively display the table names, column names, and values.

# It needs to achieve the following effect: If the tables, columns, and values in the schema are replaced with abstract mask tokens, and the tables, columns, and values in your generated question are synchronously replaced with the same abstract masks, the resulting masked question combined with the masked schema must still provide enough structural information to generate the correct masked SQL. When unmasked, this SQL must be perfectly identical to the original SQL.
# '''

# system_prompt_zh = '''# Role
# 你是一个 Text-to-SQL 领域的“自然语言结构化转换专家”。你的任务是将用户口语化的、复杂的自然语言查询（NLQ），基于提供的 Database Schema 和目标 SQL，转换为一种**高度格式化、模板化的中间态查询语言**。

# # Core Objective
# 转换后的句子必须具备极强的“结构稳定性”。即：如果将句子中的表名、列名、具体值全部使用占位符（如 `[TABLE]`, `[COLUMN]`, `[VALUE]`）进行掩码，掩码后的骨架句子依然符合英语语法，且能完美、无歧义地映射为具体的 SQL 语法树（AST）。

# # Structural Rules (句子结构约束)
# 请严格遵循以下句式规范进行重写：

# 1. **查询目标 (SELECT & Aggregation):**
#    - **基础查询：** `Find the [COLUMN] of the [TABLE] ...`
#    - **聚合查询：** 必须将 SQL 中的聚合函数翻译为固定的显式自然语言词汇，并使用 `of` 连接。
#      - `AVG()` -> `the average of`
#      - `COUNT()` -> `the number of` (或 `the total count of`)
#      - `MAX()` -> `the maximum of`
#      - `MIN()` -> `the minimum of`
#      - `SUM()` -> `the sum of`
#    - *示例：* `Find the average of the rating_score of the ratings ...`

# 2. **表与列的关系 (所属结构):**
#    - 必须严格使用所属结构表达。
#    - *句式：* `the [COLUMN_NAME] of the [TABLE_NAME]` (绝对不要写成 "[TABLE] [COLUMN]" 的紧凑组合)。

# 3. **列与值的关系 (主系表/动宾结构):**
#    - 必须是非常简单的基础句型，禁止使用复杂的从句修饰，确保掩码后 `A is B` 或 `A verb B` 结构依然成立。
#    - *支持的句式：*
#      - `... is equal to [VALUE]`
#      - `... is greater than [VALUE]`
#      - `... is between [VALUE_1] and [VALUE_2]`
#      - `... contains [VALUE]`

# 4. **条件连接 (WHERE / AND / OR):**
#    - 使用明确的逻辑连接词和括号式的从句结构。
#    - *句式：* `where condition 1 is that ( [列值关系句子] ), and condition 2 is that ( [列值关系句子] ).`

# 5. **分组 (GROUP BY):**
#    - *句式：* `grouped by the [COLUMN] of the [TABLE]`

# 6. **排序与限制 (ORDER BY / LIMIT):**
#    - *句式：* `Sort the results by the [COLUMN] of the [TABLE] in [ASC/DESC] order, and limit to [NUMBER] results.`

# # Input Format
# - Database Schema: {用户提供的 Schema}
# - User Query: {用户提供的原始自然语言}
# - Target SQL: {目标 SQL}

# # Output format
# **[严格约束]** 只输出最终调整后的英文结构化句子。不要包含任何解释性文本，不要包含任何前缀，**不要使用 Markdown 代码块标记（如 ```）**。
# '''

system_prompt_zh = '''# Role
你是一个 Text-to-SQL 领域的“自然语言结构化转换专家”。你的任务是将用户口语化的、复杂的自然语言查询（NLQ），基于提供的 Database Schema 和目标 SQL，转换为一种**高度格式化、模板化的中间态查询语言**。

# Core Objective
转换后的句子必须具备极强的“结构稳定性”。即：如果将句子中的表名、列名、具体值全部使用占位符（如 `[TABLE]`, `[COLUMN]`, `[VALUE]`）进行掩码，掩码后的骨架句子依然不影响用户的理解，使得用户能够根据掩码后的自然语言查询，即可写出对应的掩码SQL。

'''

example_user1 = '''- Database Schema:
CREATE TABLE head\n(\n\t\"age\" real, -- Example Values: `67.0`, `68.0`, `69.0`, `52.0`, `53.0`   \n\t\"head_ID\" int, --  \n\tPRIMARY KEY (\"head_ID\"),\n);
- SQL:
SELECT count(*) FROM head WHERE age > 56 ;
'''

example_assist1 = '''How many records are there in the head table where age is greater than 56?'''

example_user2 = '''- Database Schema:
CREATE TABLE lists_users
(
    user_id INTEGER not null, -- Example Values: `(2385,)`, `(15264,)`, `(15344,)`   
    foreign key (list_id) references lists(list_id),
    foreign key (user_id) references lists(user_id),
    primary key (user_id, list_id),
    list_id INTEGER not null, --  
);

CREATE TABLE lists
(
    user_id INTEGER references lists_users (user_id), -- Example Values: `(88260493,)`, `(45204418,)`, `(48905025,)`   
    list_id INTEGER not null primary key,
    list_title TEXT, -- Example Values: `average`, `Score`, `100 Greatest Living American Filmmakers`, `100`   
);

CREATE TABLE movies
(
    movie_id INTEGER not null primary key,
    movie_title TEXT, -- Example Values: `('La Antena',)`, `('Elementary Particles',)`, `("It's Winter",)`  | Column Description: Name of the movie 
);

CREATE TABLE ratings
(
    foreign key (user_id) references lists_users(user_id),
    foreign key (user_id) references lists_users(user_id),
    rating_id INTEGER, --  
    user_id INTEGER, -- Example Values: `(41579158,)`, `(85981819,)`, `(4208563,)`   
    foreign key (movie_id) references movies(movie_id),
    foreign key (rating_id) references ratings(rating_id),
    rating_score INTEGER, -- Example Values: `3`, `2`, `4`, `5`, `1`  | Column Description: Rating score ranging from 1 (lowest) to 5 (highest) | Value Description: The score is proportional to the user's liking. The higher the score is, the more the user likes the movie
    foreign key (user_id) references ratings_users(user_id),
    movie_id INTEGER, -- Example Values: `(1066,)`, `(1067,)`, `(1068,)`   
    rating_timestamp_utc TEXT, -- Example Values: `('2017-06-10 12:38:33',)`, `('2014-08-15 23:42:31',)`, `('2014-01-30 13:21:57',)`   
); 
What are the names of the movie that was rated by the user between 1/1/2013 to 12/31/2013 by the user who created the list \"100 Greatest Living American Filmmakers\"? Calculate for the average rating score of those movies in 2013. Between 1/1/2013 to 12/31/2013 refer to rating_timestamp_utc; 100 Greatest Living American Filmmakers refer to list_title; average rating score refer to DIVIDE( ADD(rating_score where rating_timestamp_utc = '1/1/2013-12/31/2013'), COUNT(rating_timestamp_utc = '1/1/2013-12/31/2013'))
- SQL:
"SELECT T2.movie_title FROM ratings AS T1 INNER JOIN movies AS T2 ON T1.movie_id = T2.movie_id INNER JOIN lists AS T3 ON T3.user_id = T1.user_id WHERE T1.rating_timestamp_utc BETWEEN '2013-01-01' AND '2013-12-31' AND T3.list_title = '100 Greatest Living American Filmmakers' ;"
'''

example_assist2 = '''Find the movie_title of the movies where the rating_timestamp_utc of the ratings is between '2013-01-01' and '2013-12-31', and the list_title of the lists is '100 Greatest Living American Filmmakers'.'''

def get_sql2nl_messages(input_str):
    messages=[
                {"role": "system", "content": system_prompt_zh},
                {"role": "user", "content": example_user1},
                {"role": "assistant", "content": example_assist1},
                {"role": "user", "content": example_user2},
                {"role": "assistant", "content": example_assist2},
                {"role": "user", "content": input_str}
            ]
    return messages