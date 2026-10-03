system_prompt_zh = '''我将提供给你一个schema linking任务的结果，请你检查该结果是否需要修订，如果需要修订，请修订后按照相同格式输出。你需要从几个角度检查。
1. 当前schema linking结果存不存在遗漏，即问题中的实体是否都被成功提取出来并且与数据库schema关联上。
2. 当前schema linking 结果存不存在多余的部分，即提取的实体中，包不包含形容词、比较词等。

3.The `entity_type` MUST be exactly one of: `"table"`, `"column"`, or `"cell_value"`.
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

example_user1 = '''- Database Schema:
CREATE TABLE ratings
(
	movie_id INTEGER, -- Example Values: `(1066,)`, `(1067,)`, `(1068,)`   
	foreign key (user_id) references ratings_users(user_id),
	user_has_payment_method INTEGER, -- Example Values: `0`, `1`   
	user_eligible_for_trial INTEGER, -- Example Values: `1`, `0`   
	rating_id INTEGER, --  
	rating_score INTEGER, -- Example Values: `3`, `2`, `4`, `5`, `1`  | Column Description: Rating score ranging from 1 (lowest) to 5 (highest) | Value Description: The score is proportional to the user's liking. The higher the score is, the more the user likes the movie
	foreign key (rating_id) references ratings(rating_id),
);

- User's question:
Count the number of ratings where the movie_id is 1269, the rating_score is less than or equal to 2, the user is eligible for a trial (user_eligible_for_trial = 1), and the user has a payment method (user_has_payment_method = 1).
- Reference SQL:
SELECT COUNT(*) FROM ratings WHERE movie_id = 1269 AND rating_score <= 2 AND user_eligible_for_trial = 1 AND user_has_payment_method = 1 ;

[
  {"entity": "ratings", "entity_type": "table", "table": "ratings", "column": null, "cell_value": null},
  {"entity": "1269", "entity_type": "cell_value", "table": "ratings", "column": "movie_id", "cell_value": "1269"},
  {"entity": "2", "entity_type": "cell_value", "table": "ratings", "column": "rating_score", "cell_value": "2"},
  {"entity": "1", "entity_type": "cell_value", "table": "ratings", "column": "user_eligible_for_trial", "cell_value": "1"},
  {"entity": "1", "entity_type": "cell_value", "table": "ratings", "column": "user_has_payment_method", "cell_value": "1"}
]
'''

example_assist1 = '''
[
  {"entity": "ratings", "entity_type": "table", "table": "ratings", "column": null, "cell_value": null},
  {"entity": "movie_id", "entity_type": "column", "table": "ratings", "column": "movie_id", "cell_value": null},
  {"entity": "1269", "entity_type": "cell_value", "table": "ratings", "column": "movie_id", "cell_value": "1269"},
  {"entity": "rating_score", "entity_type": "column", "table": "ratings", "column": "rating_score", "cell_value": null},
  {"entity": "2", "entity_type": "cell_value", "table": "ratings", "column": "rating_score", "cell_value": "2"},
  {"entity": "user_eligible_for_trial", "entity_type": "column", "table": "ratings", "column": "user_eligible_for_trial", "cell_value": null},
  {"entity": "1", "entity_type": "cell_value", "table": "ratings", "column": "user_eligible_for_trial", "cell_value": "1"},
  {"entity": "user_has_payment_method", "entity_type": "column", "table": "ratings", "column": "user_has_payment_method", "cell_value": null},
  {"entity": "1", "entity_type": "cell_value", "table": "ratings", "column": "user_has_payment_method", "cell_value": "1"}
]
'''

example_user2 = '''- Database Schema:
CREATE TABLE movies
(
	movie_title TEXT, -- Example Values: `('La Antena',)`, `('Elementary Particles',)`, `("It's Winter",)`  | Column Description: Name of the movie 
	movie_id INTEGER not null primary key,
);

CREATE TABLE ratings_users
(
	user_id INTEGER references lists_users (user_id), -- Example Values: `(41579158,)`, `(68654088,)`, `(84114365,)`   
);

CREATE TABLE ratings
(
	rating_score INTEGER, -- Example Values: `3`, `2`, `4`, `5`, `1`  | Column Description: Rating score ranging from 1 (lowest) to 5 (highest) | Value Description: The score is proportional to the user's liking. The higher the score is, the more the user likes the movie
	rating_id INTEGER, --  
	user_trialist INTEGER, -- Example Values: `0`, `1`  | Column Description: whether user was a tralist when he rated the movie | Value Description: 1 = the user was a trialist when he rated the movie  0 = the user was not a trialist when he rated the movie
	foreign key (movie_id) references movies(movie_id),
	movie_id INTEGER, -- Example Values: `(1066,)`, `(1067,)`, `(1068,)`  | Column Description: Movie ID related to the rating 
	foreign key (user_id) references ratings_users(user_id),
	foreign key (user_id) references ratings_users(user_id),
	foreign key (rating_id) references ratings(rating_id),
	user_id INTEGER, -- Example Values: `(41579158,)`, `(85981819,)`, `(4208563,)`   
);
- User's question:
Count the number of users who are not trialists (user_trialist = 0), gave a rating score of 2 or less, and rated the movie titled 'The South'.
- Reference SQL:
SELECT COUNT(T2.user_id) FROM movies AS T1 INNER JOIN ratings AS T2 ON T1.movie_id = T2.movie_id WHERE T2.user_trialist = 0 AND T2.rating_score <= 2 AND T1.movie_title = 'The South' ;

[
  {"entity": "users", "entity_type": "table", "table": "ratings_users", "column": null, "cell_value": null},
  {"entity": "not trialists", "entity_type": "column", "table": "ratings", "column": "user_trialist", "cell_value": null},
  {"entity": "0", "entity_type": "cell_value", "table": "ratings", "column": "user_trialist", "cell_value": "0"},
  {"entity": "rating score", "entity_type": "column", "table": "ratings", "column": "rating_score", "cell_value": null},
  {"entity": "2", "entity_type": "cell_value", "table": "ratings", "column": "rating_score", "cell_value": "2"},
  {"entity": "movie", "entity_type": "table", "table": "movies", "column": null, "cell_value": null},
  {"entity": "The South", "entity_type": "cell_value", "table": "movies", "column": "movie_title", "cell_value": "The South"}
]
'''

example_assist2 = '''[
  {"entity": "users", "entity_type": "column", "table": "ratings", "column": "user_id", "cell_value": null},
  {"entity": "trialists", "entity_type": "column", "table": "ratings", "column": "user_trialist", "cell_value": null},
  {"entity": "user_trialist", "entity_type": "column", "table": "ratings", "column": "user_trialist", "cell_value": null},
  {"entity": "0", "entity_type": "cell_value", "table": "ratings", "column": "user_trialist", "cell_value": "0"},
  {"entity": "rating score", "entity_type": "column", "table": "ratings", "column": "rating_score", "cell_value": null},
  {"entity": "2", "entity_type": "cell_value", "table": "ratings", "column": "rating_score", "cell_value": "2"},
  {"entity": "rated", "entity_type": "table", "table": "ratings", "column": null, "cell_value": null},
  {"entity": "movie titled", "entity_type": "column", "table": "movies", "column": "movie_title", "cell_value": null},
  {"entity": "The South", "entity_type": "cell_value", "table": "movies", "column": "movie_title", "cell_value": "The South"}
]
'''

def get_schema_linking_revise_template(input_str):
    messages=[
                {"role": "system", "content": system_prompt_zh},
                {"role": "user", "content": example_user1},
                {"role": "assistant", "content": example_assist1},
                {"role": "user", "content": example_user2},
                {"role": "assistant", "content": example_assist2},
                {"role": "user", "content": input_str}
            ]
    return messages