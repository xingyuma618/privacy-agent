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
Which movies did the company Paramount Pictures produce in 2000?

[MASKED DB SCHEMA]:
CREATE TABLE table_1 (
  col_6 INTEGER default NULL references table_3,
  col_2 INTEGER default NULL references table_2
);

CREATE TABLE table_2 (
  col_3 TEXT DEFAULT NULL,
  col_4 DATE DEFAULT NULL,
  col_2 INTEGER NOT NULL PRIMARY KEY
);

CREATE TABLE table_3 (
  col_5 TEXT DEFAULT NULL,
  col_6 INTEGER NOT NULL,
  PRIMARY KEY (col_6)
);

[MASKED QUESTION]:
Which table_2 did the table_3 val_1 produce in val2?"""

assistant_1 = """Thought: I will first analyze the masked query: "Which table_2 did the table_3 val_1 produce in val2?". The phrase "the table_3 val_1" makes no logical sense, and it is entirely unclear what the relationship between `val_1` and `table_3` is. Furthermore, "table_3 val_1 produce in val2" makes it difficult to deduce any association with `table_2`.
This kind of masked phrasing will make it very difficult for downstream Text-to-SQL models to accurately capture table join paths and specific filtering conditions. Therefore, the masked query cannot be used directly to generate SQL statements. I should first call the `query_rewrite` tool to adjust the sentence structure of the original question before proceeding with subsequent masking operations. Since no rewrite has been attempted yet, I will pass the original query to the rewrite tool.
Action:
<tool_call>
{"name": "query_rewrite", "arguments": {"query_to_rewrite": "Which movies did the company Paramount Pictures produce in 2000?"}}
</tool_call>"""



user_2 = """[DB SCHEMA]:
CREATE TABLE conditions
(
  PATIENT TEXT, -- Example Values: `('71949668-1c2e-43ae-ab0a-64654608defb',)`, `('c2caaace-9119-4b2d-a2c3-4040f5a9cf32',)`, `('96b24072-e1fe-49cd-a22a-6dfb92c3994c',)`   
  START DATE, -- Example Values: `('2009-01-08',)`, `('2010-10-16',)`, `('2013-02-07',)`   
  DESCRIPTION TEXT, -- Example Values: `('Acute bronchitis (disorder)',)`, `('Cystitis',)`, `('Normal pregnancy',)`   
);

[USER QUESTION]:
How many distinct patients in the conditions table have a condition description of 'Diabetes' and a start date in the year 1988?

[entity extract]:
"entities": [
    {
      "entity": "patient",
      "entity_type": "column",
      "table": "conditions",
      "column": "PATIENT",
      "cell_value": null
    },
    {
      "entity": "conditions",
      "entity_type": "table",
      "table": "conditions",
      "column": null,
      "cell_value": null
    },
    {
      "entity": "Diabetes",
      "entity_type": "cell_value",
      "table": "conditions",
      "column": "DESCRIPTION",
      "cell_value": "Diabetes"
    },
    {
      "entity": "start date",
      "entity_type": "column",
      "table": "conditions",
      "column": "START",
      "cell_value": null
    }
  ]

[MASKED DB SCHEMA]:
CREATE TABLE table_1 (
  `col_1` TEXT DEFAULT NULL,
  `col_2` INTEGER NOT NULL,
  PRIMARY KEY (`col_2`)
);

[MASKED QUESTION]:
How many distinct patients in the table_1 table have a condition col_3 of 'val_1' and a col_2 in the year 1988?"""



assistant_2 = """Thought: I will analyze the masked query. I noticed that the words "patients" and "1988" were not successfully masked. 
"patients" failed to mask because the extracted entity was singular ("patient"), while the query used the plural ("patients"), leading to a string matching failure during entity replacement.
"1988" failed to mask because it was entirely missed during the entity extraction step. Based on this analysis, I need to amend the "patients" entity in the entities list to match the query, append the missing "1988" entity, and pass the updated list to `apply_typed_masking` to reapply the masking.
Action:
<tool_call>
{"name": "apply_typed_masking", "arguments": {
  "query": "How many distinct patients in the conditions table have a condition description of 'Diabetes' and a start date in the year 1988?",
  "entities": [
    {
      "entity": "patients",
      "entity_type": "column",
      "table": "conditions",
      "column": "PATIENT",
      "cell_value": null
    },
    {
      "entity": "conditions",
      "entity_type": "table",
      "table": "conditions",
      "column": null,
      "cell_value": null
    },
    {
      "entity": "Diabetes",
      "entity_type": "cell_value",
      "table": "conditions",
      "column": "DESCRIPTION",
      "cell_value": "Diabetes"
    },
    {
      "entity": "start date",
      "entity_type": "column",
      "table": "conditions",
      "column": "START",
      "cell_value": null
    },
    {
      "entity": "1988",
      "entity_type": "cell_value",
      "table": "conditions",
      "column": "START",
      "cell_value": "1988"
    }
  ]
}}
</tool_call>"""


user_3 = """[DB SCHEMA]:
CREATE TABLE frpm
(
  `County Name` TEXT null, -- Example Values: `Alameda`   
  `Enrollment (K-12)` REAL null, -- Example Values: `(1087.0,)`, `(395.0,)`, `(244.0,)`  | Column Description: Enrollment (K-12) | Value Description: K-12: 1st grade - 12nd grade
  `Free Meal Count (K-12)` REAL null, -- Example Values: `(565.0,)`, `(186.0,)`, `(134.0,)`  | Column Description: Free Meal Count (K-12) | Value Description: eligible free rate = Free Meal Count / Enrollment
  PRIMARY KEY (CDSCode),
  CDSCode TEXT not null, --  
);

[USER QUESTION]:
What is the maximum value of Free Meal Count (K-12) divided by Enrollment (K-12) in the frpm table where the County Name is 'Alameda'?

[MASKED DB SCHEMA]:
CREATE TABLE table_1 (
  `col_1` TEXT null,
  `col_2` REAL null,
  `col_3` REAL null,
  col_4 TEXT not null,
  PRIMARY KEY (col_4)
);

[MASKED QUESTION]:
What is the maximum value of col_3 divided by col_2 in the table_1 table where the col_1 is 'val_1'?"""


assistant_3 = """Thought: I will analyze the masked query. The privacy entities in this query have been successfully masked, and the grammatical structure remains intact. This does not hinder our ability to derive the SQL statement based on the masked query and schema. We can easily deduce that "maximum value of col_3 divided by col_2 in the table_1" translates to `SELECT MAX(col_3 / col_2) FROM table_1`, and "where the col_1 is 'val_1'" translates to `WHERE col_1 = 'val_1'`. Because the masking is successful and logically sound, I can proceed directly to generating the SQL statement using the `sql_generate` tool.
Action:
<tool_call>
{"name": "sql_generate", "arguments": {
  "masked_query": "What is the maximum value of col_3 divided by col_2 in the table_1 table where the col_1 is 'val_1'?",
  "masked_schema_str": "CREATE TABLE table_1 (\n\t`col_1` TEXT null,\n\t`col_2` REAL null,\n\t`col_3` REAL null,\n\tcol_4 TEXT not null,\n\tPRIMARY KEY (col_4)\n);"
}}
</tool_call>"""

evaluate_mask_messages = [
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
    },
    {
        "role": "user",
        "content": user_3
    },
    {
        "role": "assistant",
        "content": assistant_3
    }
]

# 在这里追加你实际运行时的最新用户输入
# messages.append({"role": "user", "content": "..."})