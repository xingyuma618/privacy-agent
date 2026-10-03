
user_1 = """[DB SCHEMA]:
CREATE TABLE movie_company
(
    company_id INTEGER default NULL references production_company, -- Example Values: `(14,)`, `(59,)`, `(1,)` | Column Name Meaning: company id | Column Description: the id of the company that produced the movie Maps to production_company(company_id) | Value Description: If movies with different movie_id have the same company_id, it means these movies were made by the same company.
    movie_id INTEGER default NULL references movie, -- Example Values: `(5,)`, `(11,)`, `(12,)`   
);

CREATE TABLE movie
(
    title TEXT DEFAULT NULL, -- Example Values: `Fantasia 2000`   
    movie_id INTEGER NOT NULL PRIMARY KEY,
    release_date DATE DEFAULT NULL, -- Example Values: `('1995-12-09',)`, `('1977-05-25',)`, `('2003-05-30',)`   
);

CREATE TABLE production_company
(
    company_name TEXT DEFAULT NULL, -- Example Values: `Paramount Pictures`, `Paramount`   
    PRIMARY KEY (company_id),
    company_id INTEGER NOT NULL, -- Example Values: `(1,)`, `(2,)`, `(3,)`   
);

[USER QUESTION]:
Which movies did the company Paramount Pictures produce in 2000?"""

assistant_1 = """Thought: Let me first predict the masked query. It would likely be: "Which [table] did the [column] [val_1] produce in [val_2]?". Obviously, this query is problematic because it is very difficult to discern the relationship between the table and the column, or how val_1 and val_2 should be organized into SQL query conditions. Based on this finding, we should first call the `query_rewrite` tool to adjust the query structure before any extraction or masking occurs.
Action:
<tool_call>
{"name": "query_rewrite", "arguments": {"query_to_rewrite": "Which movies did the company Paramount Pictures produce in 2000?"}}
</tool_call>"""

user_2 = """[DB SCHEMA]:
CREATE TABLE frpm
(
    `County Name` TEXT null, -- Example Values: `Alameda`   
    `Enrollment (K-12)` REAL null, -- Example Values: `(1087.0,)`, `(395.0,)`, `(244.0,)`  | Column Description: Enrollment (K-12) | Value Description: K-12: 1st grade - 12nd grade
    `Free Meal Count (K-12)` REAL null, -- Example Values: `(565.0,)`, `(186.0,)`, `(134.0,)`  | Column Description: Free Meal Count (K-12) | Value Description: eligible free rate = Free Meal Count / Enrollment
    PRIMARY KEY (CDSCode),
    CDSCode TEXT not null, --  
);

[USER QUESTION]:
What is the maximum value of Free Meal Count (K-12) divided by Enrollment (K-12) in the frpm table where the County Name is 'Alameda'?"""

assistant_2 = """Thought: Let me first predict the masked query. It should be: "What is the maximum value of col_3 divided by col_2 in the table_1 table where the col_1 is 'val_1'?". Although the privacy entities in this query are conceptually masked, it does not hinder us from writing the SQL statement based on this predicted mask structure. We can easily deduce that "maximum value of col_3 divided by col_2 in the table_1" translates to `SELECT MAX(col_3 / col_2) FROM table_1`, and "where the col_1 is 'val_1'" translates to `WHERE col_1 = 'val_1'`. Therefore, direct masking will not affect our ability to write the corresponding SQL statement. I can safely proceed by directly calling the `entity_extract` tool to extract the privacy entities for the masking operation, without needing to rewrite the query.
Action:
<tool_call>
{"name": "entity_extract", "arguments": {"query_to_extract": "What is the maximum value of Free Meal Count (K-12) divided by Enrollment (K-12) in the frpm table where the County Name is 'Alameda'?"}}
</tool_call>"""

evaluate_structure_messages = [

    {
        "role": "user",
        "content": user_1
    },
    {
        "role": "assistant",
        "content": assistant_1
    },
    {
        "role": "user",
        "content": user_2
    },
    {
        "role": "assistant",
        "content": assistant_2
    }
]

# 在实际调用 API 前，将当前用户的真实输入追加到列表末尾：
# current_user_input = "[DB SCHEMA]: ... \n[USER QUESTION]: ..."
# messages.append({"role": "user", "content": current_user_input})