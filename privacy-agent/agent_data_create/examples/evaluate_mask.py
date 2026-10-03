
system_prompt = '''你是一个句子结构分析专家，我将提供给 Original Database Schema，Masked Question。其中masked question 是对original question进行隐私实体掩码得到。
你需要分析掩码后question和schema是否合理。
- 如果存在语句结构不完整或与原始问题差异较大，导致看不出masked question要查询什么，说明需要调用query_rewrite先调整query语句结构。
- 如果存在实体漏掩码，说明entity_extract步骤提取的实体不完整，你需要将你观察到的漏掉的实体直接补充到entities中，并且再次调用apply_typed_masking
【输出要求】
在决定调用任何工具前，你必须先在回答中输出你的思考过程！每次回复都应该符合以下逻辑：
Thought: [在这里写下你的思考过程。分析当前的输入、前一步的Observation，以及你接下来需要做哪一步操作，为什么。]
Action: [通过触发相应的 function call 来执行你的行动]

'''

## 掩码后结构评估， 语义结构不对示例
user_1 = '''[DB SCHEMA]:
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
Which table_2 did the table_3 val_1 produce in val2?

'''

assist_1 = '''Thought: 我先分析一下掩码后的query "Which table_2 did the table_3 val_1 produce in val2?" 这句话，"the table_3 val_1" 完全看不出来是想表达什么意思，不确定val_1和table_3之间是什么关系，而且table_3 val_1 produce in val2也很难推断出与table_2之间有什么关联。
这种掩码表达会让下游的 Text-to-SQL 模型难以确切地抓取表间关联路径和具体的过滤条件。因此掩码后的query不能直接用来生成SQL语句，应该先调用query_rewrite工具对问题进行query句式调整，然后再进行后续的掩码操作。由于之前没有尝试过改写，因此我先仅传入原始query给query改写工具看看能不能改写正确。
Action:
<tool_call>
{"name": "query_rewrite", "arguments": {"query_to_rewrite": "Which movies did the company Paramount Pictures produce in 2000?"}}
</tool_call>
'''


## 实体漏掩码示例

user_2 = '''[DB SCHEMA]:
CREATE TABLE conditions
(
	PATIENT TEXT, -- Example Values: `('71949668-1c2e-43ae-ab0a-64654608defb',)`, `('c2caaace-9119-4b2d-a2c3-4040f5a9cf32',)`, `('96b24072-e1fe-49cd-a22a-6dfb92c3994c',)`   
	START DATE, -- Example Values: `('2009-01-08',)`, `('2010-10-16',)`, `('2013-02-07',)`   
	DESCRIPTION TEXT, -- Example Values: `('Acute bronchitis (disorder)',)`, `('Cystitis',)`, `('Normal pregnancy',)`   
);

[USER QUESTION]:
How many distinct patients in the conditions table have a condition description of 'Diabetes' and a start date in the year 1988?


[entity extract]
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
How many distinct patients in the table_1 table have a condition col_3 of 'val_1' and a col_2 in the year 1988?


'''

assist_2 = '''Thought: 我先分析一下掩码后的query，我发现patients，和1988没有被成功掩码。
patients漏掩码是因为提取的实体是patient而query中的实体是patients，所以在实体替换的时候，patients没有被字符串匹配上，导致掩码失败。
1988漏掩码是因为在这部分实体漏提取了，结合上述分析，我需要修订entities中的patients实体为patient，并且补充新的实体1988，得到新的entities列表传递给apply_typed_masking重新进行掩码。
Action:
<tool_call>
{"name": "apply_typed_masking", "arguments": {"query": "How many distinct patients in the conditions table have a condition description of 'Diabetes' and a start date in the year 1988?","entities": [
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
  }
  }
</tool_call>
'''

# 正确掩码示例
user_3 = '''[DB SCHEMA]:
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
What is the maximum value of col_3 divided by col_2 in the table_1 table where the col_1 is 'val_1'?
'''
assist_3 = '''Thought: 我先分析一下这个掩码后的query，这个query中的隐私实体虽然已经被mask，但是不影响我们根据当前mask后的query写出sql语句，因为我们从query中可以知道 maximum value of col_3 divided by col_2 in the table_1 就是SELECT MAX(col_3 / col_2) FROM table_1 ，而 where the col_1 is 'val_1'就是WHERE col_1 = 'val_1'，因此掩码后的query并不影响我们写出对应的SQL语句。因此我们可以直接使用sql_generate工具进行sql语句生成。
Action:
<tool_call>
{"name": "sql_generate", "arguments": {
  "masked_query": "What is the maximum value of col_3 divided by col_2 in the table_1 table where the col_1 is 'val_1'?",
  "masked_schema_str": "CREATE TABLE table_1 (\n\t`col_1` TEXT null,\n\t`col_2` REAL null,\n\t`col_3` REAL null,\n\tcol_4 TEXT not null,\n\tPRIMARY KEY (col_4)\n);"
}
</tool_call>
'''
