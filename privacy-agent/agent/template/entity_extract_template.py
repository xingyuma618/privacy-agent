system_prompt_en = '''This is a Schema Linking task. I will provide you with a Database schema, a User's question, and a Reference SQL. 
Your task is to extract all key entities (words or phrases) from the User's question and map them to the database schema.

# Rules
1. Output ONLY a valid JSON array. NO markdown formatting (e.g., do NOT use ```json).
2. Every object MUST contain exactly 5 keys: `entity`, `entity_type`, `table`, `column`, `cell_value`.
3. Extract ONLY the minimal base words or phrases representing the table, column, or cell value. You MUST STRIP AWAY all aggregations, superlatives, adjectives, and conditions (e.g., "earliest", "latest", "highest", "lowest", "average", "total").
   - *Good Example 1*: For the phrase "longest movie title", extract ONLY "movie title" as the entity, NOT "longest movie title".
4. Do NOT extract words that represent SQL logic, condition filters, null checks, logical negations, or comparison operators (e.g., "missing", "excluding", "without", "not", "or less", "more than", "at least", "equal to").

5. The `entity_type` MUST be exactly one of: `"table"`, `"column"`, or `"cell_value"`.
   - If `entity_type = "table"`: Fill the `table` name. `column` and `cell_value` MUST be ''.
   - If `entity_type = "column"`: Fill BOTH `table` and `column` names. `cell_value` MUST be ''.
   - If `entity_type = "cell_value"`: Fill ALL THREE: `table`, `column`, and `cell_value`. The `entity` MUST ONLY be the pure value (e.g., "2", "0", "The South"), stripped of any surrounding narrative.

# Output Format Example
"entities":
[
  {"entity": "Entity1", "entity_type": "table", "table": "table1_name", "column": '', "cell_value": ''},
  {"entity": "Entity2", "entity_type": "column", "table": "table1_name", "column": "column2_name", "cell_value": ''},
  {"entity": "Entity3", "entity_type": "cell_value", "table": "table2_name", "column": "column3_name", "cell_value": "value3"}
]
'''

example_user1 = '''
- Database Schema:
CREATE TABLE movies\n(\n\tmovie_id INTEGER not null primary key,\n\tmovie_popularity INTEGER, -- Example Values: `(105,)`, `(23,)`, `(21,)`  | Column Description: Number of Mubi users who love this movie \n\tdirector_name TEXT, -- Example Values: `Stanley Kubrick`   \n);\n\n
- User's Question:
Calculate the average movie popularity for movies directed by Stanley Kubrick.

'''

example_assist1 = '''"entities":
[
  {"entity": "movie popularity", "entity_type": "column", "table": "movies", "column": "movie_popularity", "cell_value": ''},
  {"entity": "movies", "entity_type": "table", "table": "movies", "column": '', "cell_value": ''},
  {"entity": "Stanley Kubrick", "entity_type": "cell_value", "table": "movies", "column": "director_name", "cell_value": "Stanley Kubrick"}
]
'''

example_user2 = '''- Database Schema:
CREATE TABLE movies
(
	movie_popularity INTEGER, -- Example Values: `(105,)`, `(23,)`, `(21,)`   
	movie_title TEXT, -- Example Values: `('La Antena',)`, `('Elementary Particles',)`, `("It's Winter",)`  | Column Description: Name of the movie 
	movie_release_year INTEGER, -- Example Values: `(2007,)`, `(2006,)`, `(2005,)`  | Column Description: Release year of the movie 
	movie_id INTEGER not null primary key,
);

- User's question:
Find the movie title and release year of the movie with the highest movie_popularity value, based on the length of the movie_popularity number when treated as text, and return only the top result.

'''

example_assist2 = '''"entities":
[
  {"entity": "movie title", "entity_type": "column", "table": "movies", "column": "movie_title", "cell_value": ''},
  {"entity": "release year", "entity_type": "column", "table": "movies", "column": "movie_release_year", "cell_value": ''},
  {"entity": "movie", "entity_type": "table", "table": "movies", "column": '', "cell_value": ''},
  {"entity": "movie_popularity", "entity_type": "column", "table": "movies", "column": "movie_popularity", "cell_value": ''}
]
'''


example_user4 = '''- Database Schema:
CREATE TABLE movies\n(\n\tmovie_popularity INTEGER, -- Example Values: `(105,)`, `(23,)`, `(21,)`   \n\tmovie_title TEXT, -- Example Values: `('La Antena',)`, `('Elementary Particles',)`, `(\"It's Winter\",)`   \n\tmovie_id INTEGER not null primary key,\n\tmovie_release_year INTEGER, -- Example Values: `(2007,)`, `(2006,)`, `(2005,)`  | Column Description: Release year of the movie \n);\n\n
- User's Question:
Find the movie title with the highest popularity among movies released in 1945.
'''

example_assist4 = '''"entities":
[
  {"entity": "movie title", "entity_type": "column", "table": "movies", "column": "movie_title", "cell_value": ''},
  {"entity": "popularity", "entity_type": "column", "table": "movies", "column": "movie_popularity", "cell_value": ''},
  {"entity": "movies", "entity_type": "table", "table": "movies", "column": '', "cell_value": ''},
  {"entity": "1945", "entity_type": "cell_value", "table": "movies", "column": "movie_release_year", "cell_value": "1945"}
]
'''


system_prompt_zh = '这是一个实体提取任务，你需要提取用户自然语言查询中的实体，并且与数据库schema关联起来。'

def get_entity_extract_template(input_str):
    messages=[
        {"role": "system", "content": system_prompt_zh},
        # {"role": "user", "content": example_user1},
        # {"role": "assistant", "content": example_assist1},
        # {"role": "user", "content": example_user2},
        # {"role": "assistant", "content": example_assist2},
        # {"role": "user", "content": example_user4},
        # {"role": "assistant", "content": example_assist4},
        # 放入当前待处理的问题
        {"role": "user", "content": input_str}
    ]
    return messages