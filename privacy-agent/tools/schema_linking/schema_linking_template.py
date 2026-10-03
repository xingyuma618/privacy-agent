system_prompt_zh = '''我将提供给你 Database schema, User's question 以及 Reference SQL。你需要提取出User's question中的所有关键实体（单词或短语），并且给出实体与数据库schema得关系。
# 规则
1. Output ONLY a valid JSON array.
2. **Fixed Schema**: Every object MUST contain exactly 5 keys: `entity`, `entity_type`, `table`, `column`, `cell_value`. NEVER omit keys; 
3. 你只需要提取能代表表名、列名或者cell value的单词或短语即可，不要有多余的部分，例如：
    - "longest movie title refers to MAX(LENGTH(movie_title))", 很明显只要movie title就知道对应列名movie_title，因此“longest”，“refers to MAX(LENGTH(movie_title))”等部分都不要。
3. Reference SQL 是你进行正确映射的最重要参考。
4. **Hierarchical Mapping Logic (Strict)**:
   - If `entity_type = "table"`: Fill `table` name. `column` and `cell_value` MUST be `null`.
   - If `entity_type = "column"`: Fill BOTH `table` and `column` names. `cell_value` MUST be `null`.
   - If `entity_type = "cell_value"`: Fill ALL THREE: `table`, `column`, and `cell_value`.

# 输出格式示例
[
  {"entity": "Entity1", "entity_type": "table", "table": "table1_name", "column": null, "cell_value": null},
  {"entity": "Entity2", "entity_type": "column", "table": "table1_name", "column": "column2_name", "cell_value": null},
  {"entity": "Entity3", "entity_type": "cell_value", "table": "table2_name", "column": "column3_name", "cell_value": "value3"}
]

'''
system_prompt_en = '''This is a Schema Linking task. I will provide you with a Database schema, a User's question, and a Reference SQL. 
Your task is to extract all key entities (words or phrases) from the User's question and map them to the database schema.

# Rules
1. Output ONLY a valid JSON array. NO markdown formatting (e.g., do NOT use ```json).
2. Every object MUST contain exactly 5 keys: `entity`, `entity_type`, `table`, `column`, `cell_value`.
3. Extract ONLY the minimal base words or phrases representing the table, column, or cell value. You MUST STRIP AWAY all aggregations, superlatives, adjectives, and conditions (e.g., "earliest", "latest", "highest", "lowest", "average", "total").
   - *Good Example 1*: For the phrase "longest movie title", extract ONLY "movie title" as the entity, NOT "longest movie title".
4. Do NOT extract words that represent SQL logic, condition filters, null checks, logical negations, or comparison operators (e.g., "missing", "excluding", "without", "not", "or less", "more than", "at least", "equal to").

5. The `entity_type` MUST be exactly one of: `"table"`, `"column"`, or `"cell_value"`.
   - If `entity_type = "table"`: Fill the `table` name. `column` and `cell_value` MUST be `null`.
   - If `entity_type = "column"`: Fill BOTH `table` and `column` names. `cell_value` MUST be `null`.
   - If `entity_type = "cell_value"`: Fill ALL THREE: `table`, `column`, and `cell_value`. The `entity` MUST ONLY be the pure value (e.g., "2", "0", "The South"), stripped of any surrounding narrative.

# Output Format Example
[
  {"entity": "Entity1", "entity_type": "table", "table": "table1_name", "column": null, "cell_value": null},
  {"entity": "Entity2", "entity_type": "column", "table": "table1_name", "column": "column2_name", "cell_value": null},
  {"entity": "Entity3", "entity_type": "cell_value", "table": "table2_name", "column": "column3_name", "cell_value": "value3"}
]
'''

example_user1 = '''
- Database Schema:
CREATE TABLE movies\n(\n\tmovie_id INTEGER not null primary key,\n\tmovie_popularity INTEGER, -- Example Values: `(105,)`, `(23,)`, `(21,)`  | Column Description: Number of Mubi users who love this movie \n\tdirector_name TEXT, -- Example Values: `Stanley Kubrick`   \n);\n\n
- User's Question:
Calculate the average movie popularity for movies directed by Stanley Kubrick.

- Reference SQL:
SELECT AVG(movie_popularity) FROM movies WHERE director_name = 'Stanley Kubrick' ;
'''

example_assist1 = '''[
{"entity": "movie popularity", "entity_type": "column", "table": "movies", "column": "movie_popularity", "cell_value": null},
{"entity": "movies", "entity_type": "table", "table": "movies", "column": null, "cell_value": null},
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
- Reference SQL:
SELECT movie_title, movie_release_year FROM movies ORDER BY LENGTH(movie_popularity) DESC LIMIT 1 ;
'''

example_assist2 = '''[
{"entity": "movie title", "entity_type": "column", "table": "movies", "column": "movie_title", "cell_value": null},
{"entity": "release year", "entity_type": "column", "table": "movies", "column": "movie_release_year", "cell_value": null},
{"entity": "movie", "entity_type": "table", "table": "movies", "column": null, "cell_value": null},
{"entity": "movie_popularity", "entity_type": "column", "table": "movies", "column": "movie_popularity", "cell_value": null}
]
'''

example_user3 = '''- Database Schema:
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


- User's question:
What are the names of the movie that was rated by the user between 1/1/2013 to 12/31/2013 by the user who created the list "100 Greatest Living American Filmmakers"? Calculate for the average rating score of those movies in 2013. Between 1/1/2013 to 12/31/2013 refer to rating_timestamp_utc; 100 Greatest Living American Filmmakers refer to list_title; average rating score refer to DIVIDE( ADD(rating_score where rating_timestamp_utc = '1/1/2013-12/31/2013'), COUNT(rating_timestamp_utc = '1/1/2013-12/31/2013'))
- Reference SQL:
SELECT T2.movie_title FROM ratings AS T1 INNER JOIN movies AS T2 ON T1.movie_id = T2.movie_id INNER JOIN lists AS T3 ON T3.user_id = T1.user_id WHERE T1.rating_timestamp_utc BETWEEN '2013-01-01' AND '2013-12-31' AND T3.list_title = '100 Greatest Living American Filmmakers' ;
'''

example_assist3 = '''[
    {"entity": "names of the movie", "entity_type": "column", "table": "movies", "column": "movie_title", "cell_value": null},
    {"entity": "movie", "entity_type": "table", "table": "movies", "column": null, "cell_value": null},
    {"entity": "rated", "entity_type": "table", "table": "ratings", "column": null, "cell_value": null},
    {"entity": "user", "entity_type": "column", "table": "ratings", "column": "user_id", "cell_value": null},
    {"entity": "1/1/2013", "entity_type": "cell_value", "table": "ratings", "column": "rating_timestamp_utc", "cell_value": "2013-01-01"},
    {"entity": "12/31/2013", "entity_type": "cell_value", "table": "ratings", "column": "rating_timestamp_utc", "cell_value": "2013-12-31"},
    {"entity": "list", "entity_type": "table", "table": "lists", "column": null, "cell_value": null},
    {"entity": "100 Greatest Living American Filmmakers", "entity_type": "cell_value", "table": "lists", "column": "list_title", "cell_value": "100 Greatest Living American Filmmakers"},
    {"entity": "rating score", "entity_type": "column", "table": "ratings", "column": "rating_score", "cell_value": null},
    {"entity": "rating_timestamp_utc", "entity_type": "column", "table": "ratings", "column": "rating_timestamp_utc", "cell_value": null},
    {"entity": "list_title", "entity_type": "column", "table": "lists", "column": "list_title", "cell_value": null}
]
'''

example_user4 = '''- Database Schema:
CREATE TABLE movies\n(\n\tmovie_popularity INTEGER, -- Example Values: `(105,)`, `(23,)`, `(21,)`   \n\tmovie_title TEXT, -- Example Values: `('La Antena',)`, `('Elementary Particles',)`, `(\"It's Winter\",)`   \n\tmovie_id INTEGER not null primary key,\n\tmovie_release_year INTEGER, -- Example Values: `(2007,)`, `(2006,)`, `(2005,)`  | Column Description: Release year of the movie \n);\n\n
- User's Question:
Find the movie title with the highest popularity among movies released in 1945.

- Reference SQL:
SELECT movie_title FROM movies WHERE movie_release_year = 1945 ORDER BY movie_popularity DESC LIMIT 1 ;
'''

example_assist4 = '''[
  {"entity": "movie title", "entity_type": "column", "table": "movies", "column": "movie_title", "cell_value": null},
  {"entity": "popularity", "entity_type": "column", "table": "movies", "column": "movie_popularity", "cell_value": null},
  {"entity": "movies", "entity_type": "table", "table": "movies", "column": null, "cell_value": null},
  {"entity": "1945", "entity_type": "cell_value", "table": "movies", "column": "movie_release_year", "cell_value": "1945"}
]
'''

def get_schema_linking_template(input_str):
    messages=[
                {"role": "system", "content": system_prompt_en},
                {"role": "user", "content": example_user1},
                {"role": "assistant", "content": example_assist1},
                {"role": "user", "content": example_user2},
                {"role": "assistant", "content": example_assist2},
                {"role": "user", "content": example_user4},
                {"role": "assistant", "content": example_assist4},
                {"role": "user", "content": input_str}
            ]
    return messages