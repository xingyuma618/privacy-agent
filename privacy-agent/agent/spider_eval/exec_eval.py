import os
import re
import asyncio
import sqlite3
import threading
from typing import Tuple, Any, List, Set
from itertools import product
from collections import defaultdict
from tqdm import tqdm
import random
from parse import get_all_preds_for_execution, remove_distinct
import time
import pickle as pkl
import subprocess
from itertools import chain
from tools import eval_hardness
from process_sql import Schema, get_schema, get_sql



threadLock = threading.Lock()
TIMEOUT = 60
EXEC_TMP_DIR = 'tmp/'

def permute_tuple(element: Tuple, perm: Tuple) -> Tuple:
    assert len(element) == len(perm)
    return tuple([element[i] for i in perm])


def unorder_row(row: Tuple) -> Tuple:
    return tuple(sorted(row, key=lambda x: str(x) + str(type(x))))


# unorder each row in the table
# [result_1 and result_2 has the same bag of unordered row]
# is a necessary condition of
# [result_1 and result_2 are equivalent in denotation]
def quick_rej(result1: List[Tuple], result2: List[Tuple], order_matters: bool) -> bool:
    s1 = [unorder_row(row) for row in result1]
    s2 = [unorder_row(row) for row in result2]
    if order_matters:
        return s1 == s2
    else:
        return set(s1) == set(s2)


# return whether two bag of relations are equivalent
def multiset_eq(l1: List, l2: List) -> bool:
    if len(l1) != len(l2):
        return False
    d = defaultdict(int)
    for e in l1:
        d[e] = d[e] + 1
    for e in l2:
        d[e] = d[e] - 1
        if d[e] < 0:
            return False
    return True


def get_constraint_permutation(tab1_sets_by_columns: List[Set], result2: List[Tuple]):
    num_cols = len(result2[0])
    perm_constraints = [{i for i in range(num_cols)} for _ in range(num_cols)]
    if num_cols <= 3:
        return product(*perm_constraints)

    # we sample 20 rows and constrain the space of permutations
    for _ in range(20):
        random_tab2_row = random.choice(result2)

        for tab1_col in range(num_cols):
            for tab2_col in set(perm_constraints[tab1_col]):
                if random_tab2_row[tab2_col] not in tab1_sets_by_columns[tab1_col]:
                    perm_constraints[tab1_col].remove(tab2_col)
    return product(*perm_constraints)


# check whether two denotations are correct
def result_eq(result1: List[Tuple], result2: List[Tuple], order_matters: bool) -> bool:
    if len(result1) == 0 and len(result2) == 0:
        return True
    # print(len(result1))
    # print('=============================')
    # print(len(result2))

    # if length is not the same, then they are definitely different bag of rows
    if len(result1) != len(result2):
        return False
    

    num_cols = len(result1[0])

    # if the results do not have the same number of columns, they are different
    if len(result2[0]) != num_cols:
        return False

    # unorder each row and compare whether the denotation is the same
    # this can already find most pair of denotations that are different
    if not quick_rej(result1, result2, order_matters):
        return False
    # the rest of the problem is in fact more complicated than one might think
    # we want to find a permutation of column order and a permutation of row order,
    # s.t. result_1 is the same as result_2
    # we return true if we can find such column & row permutations
    # and false if we cannot
    tab1_sets_by_columns = [{row[i] for row in result1} for i in range(num_cols)]

    # on a high level, we enumerate all possible column permutations that might make result_1 == result_2
    # we decrease the size of the column permutation space by the function get_constraint_permutation
    # if one of the permutation make result_1, result_2 equivalent, then they are equivalent
    for perm in get_constraint_permutation(tab1_sets_by_columns, result2):
        if len(perm) != len(set(perm)):
            continue
        if num_cols == 1:
            result2_perm = result2
        else:
            result2_perm = [permute_tuple(element, perm) for element in result2]
        if order_matters:
            if result1 == result2_perm:
                return True
        else:
            # in fact the first condition must hold if the second condition holds
            # but the first is way more efficient implementation-wise
            # and we use it to quickly reject impossible candidates
            if set(result1) == set(result2_perm) and multiset_eq(result1, result2_perm):
                return True
    return False


def replace_cur_year(query: str) -> str:
    return re.sub(
        "YEAR\s*\(\s*CURDATE\s*\(\s*\)\s*\)\s*", "2020", query, flags=re.IGNORECASE
    )


# get the database cursor for a sqlite database path
def get_cursor_from_path(sqlite_path: str):
    try:
        if not os.path.exists(sqlite_path):
            print("Openning a new connection %s" % sqlite_path)
        connection = sqlite3.connect(sqlite_path)
    except Exception as e:
        print(sqlite_path)
        raise e
    connection.text_factory = lambda b: b.decode(errors="ignore")
    cursor = connection.cursor()
    return cursor


async def exec_on_db_(sqlite_path: str, query: str) -> Tuple[str, Any]:
    query = replace_cur_year(query)
    cursor = get_cursor_from_path(sqlite_path)
    try:
        # print('===========exec_on_db_11111111=======\n')
        # print(query)
        # print(sqlite_path)
        # print('===========exec_on_db_22222222=======\n')


        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        cursor.connection.close()
        return "result", result
    except Exception as e:
        cursor.close()
        cursor.connection.close()
        return "exception", e

async def exec_on_db(
    sqlite_path: str, query: str, process_id: str = "", timeout: int = TIMEOUT
) -> Tuple[str, Any]:
    try:
        return await asyncio.wait_for(exec_on_db_(sqlite_path, query), timeout)
    except asyncio.TimeoutError:
        return ('exception', TimeoutError)
    except Exception as e:
        return ("exception", e)


# postprocess the model predictions to avoid execution errors
# e.g. removing spaces between ">" and "="
def postprocess(query: str) -> str:
    query = query.replace('> =', '>=').replace('< =', '<=').replace('! =', '!=')
    return query


# approximate whether p_str and g_str are semantically equivalent
# db is the database path
# we are going to evaluate whether they are equivalent in all the databases
# that are in the same directory as db
# 0 if denotationally equivalent
# 1 otherwise
# the meaning of each auxillary argument can be seen in the parser definition in evaluation.py
def eval_exec_match(db: str, p_str: str, g_str: str, plug_value: bool, keep_distinct: bool, progress_bar_for_each_datapoint: bool) -> int:
    # post-process the prediction.
    # e.g. removing spaces between ">" and "="
    p_str, g_str = postprocess(p_str), postprocess(g_str)
    if not keep_distinct:
        p_str = remove_distinct(p_str)
        g_str = remove_distinct(g_str)

    # we decide whether two denotations are equivalent based on "bag semantics"
    # https://courses.cs.washington.edu/courses/cse444/10sp/lectures/lecture16.pdf
    # if there is order by in query, then we assume order of the rows matter
    # order by might also be used to find the max/min instead of sorting,
    # but in that case the result mostly only contains one row and hence order_matters does not make a difference
    order_matters = 'order by' in g_str.lower()

    # find all databases in the same directory
    db_dir = os.path.dirname(db)
    db_paths = [os.path.join(db_dir, basename) for basename in os.listdir(db_dir) if '.sqlite' in basename]
    preds = [p_str]
    # if plug in value (i.e. we do not consider value prediction correctness)
    # enumerate all ways to plug in values in the gold query to the model predictions
    # otherwise, we only evaluate the predicted query with its own value prediction
    if plug_value:
        _, preds = get_all_preds_for_execution(g_str, p_str)
        # we did not add this line in our EMNLP work
        # this reduces "false negatives" when value is substituted
        preds = chain([p_str], preds)

    for pred in preds:

        pred_passes = 1
        # compare the gold and predicted denotations on each database in the directory
        # wrap with progress bar if required
        if progress_bar_for_each_datapoint:
            ranger = tqdm.tqdm(db_paths)
        else:
            ranger = db_paths
        
        for db_path in ranger:
            g_flag, g_denotation = asyncio.run(exec_on_db(db_path, g_str))
            p_flag, p_denotation = asyncio.run(exec_on_db(db_path, pred))
            # we should expect the gold to be succesfully executed on the database
            # assert g_flag != 'exception', 'gold query %s has error on database file %s' % (g_str, db_path)
            try:
                result_eq(g_denotation, p_denotation, order_matters=order_matters)
            except Exception as e:
                pred_passes = 0
                break
            # wrong if execution fails
            if p_flag == 'exception':
                pred_passes = 0
            
            # if denotations are not equivalent, the prediction must be wrong
            elif not result_eq(g_denotation, p_denotation, order_matters=order_matters):
                pred_passes = 0
            if pred_passes == 0:
                break

        # the model prediction has the same denotation as the gold for all databases
        if pred_passes == 1:
            return 1

    # none of the predictions passed
    return 0


def clean_space(sql):
    sql = re.sub(r'\s*\)', ')', sql)
    #聚合函数括号需要连着
    sql = re.sub(r'min\s*\(\s*', 'min(', sql)
    sql = re.sub(r'max\s*\(\s*', 'max(', sql)
    sql = re.sub(r'sum\s*\(\s*', 'sum(', sql)
    sql = re.sub(r'avg\s*\(\s*', 'avg(', sql)
    sql = re.sub(r'count\s*\(\s*', 'count(', sql)

    sql = re.sub(r'MIN\s*\(\s*', 'MIN(', sql)
    sql = re.sub(r'MAX\s*\(\s*', 'MAX(', sql)
    sql = re.sub(r'SUM\s*\(\s*', 'SUM(', sql)
    sql = re.sub(r'AVG\s*\(\s*', 'AVG(', sql)
    sql = re.sub(r'COUNT\s*\(\s*', 'COUNT(', sql)
    #非聚合函数，括号只删除左括号右边空格
    sql = re.sub(r'\(\s*', '(', sql)
    #删除多余空格
    sql = re.sub(r' +', ' ', sql)
    sql = re.sub(r'\s*,', ',', sql) 

    return sql

def find_sql(text):
    # print(text)
    sql = text
    if '```sql' in text:
        pattern = r"```sql(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        if len(matches) > 0:
            sql = matches[0].replace('\n', ' ').strip()
            sql = clean_space(sql)
            return sql
    
    if '```' in text:
        pattern = r"```(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        if len(matches) > 0:
            sql = matches[0].replace('\n', ' ').strip()
            sql = clean_space(sql)
            return sql
    

    sql = sql.replace('\n', ' ').strip()
    sql = clean_space(sql)
    return sql



if __name__ == '__main__':
    import json 
    from collections import Counter
    import pandas as pd 
    output_path = '/home/maxingyu/work/docker/DB-GPT-Hub/dbgpt_hub/output/pred/icl/zero-shot/spider/llama/70b/base_prompt/erro_case.json'
    db_ans_path = '/home/maxingyu/work/docker/DB-GPT-Hub/dbgpt_hub/output/pred/icl/zero-shot/spider/llama/70b/base_prompt/db_ans_result.json'
    db_base_path = '/home/maxingyu/work/nl2sql/evaluation/spider/spider/database'
    gold_path = '/home/maxingyu/work/docker/DB-GPT-Hub/dbgpt_hub/data/spider/spider_train.json'
    pred_path = '/home/maxingyu/work/docker/DB-GPT-Hub/dbgpt_hub/output/pred/icl/zero-shot/spider/llama/70b/base_prompt/train.sql'
    gold_data = json.load(open(gold_path, 'r'))
    pred_data = open(pred_path,'r').read().splitlines()
    erro_data = []
    erro_db_id = {}
    all_db_id = {}
    hardness_dict = {}
    hardness_dict_erro = {}
    easy_data, medium_data, hard_data, extra_data = [], [], [], []
    k = 0
    # db_id	错误率	简单题数量	中等题数量	困难数量	特别困难数量	简单题错误率	中等题错误率	困难错误率	特别困难错误率	instruction长度	表数量	列数量

    for gold_item, pred in tqdm(zip(gold_data, pred_data)):
        db_id = gold_item['db_id']
        hardness_dict[db_id] = {'easy':0, 'medium': 0, 'hard': 0, 'extra hard': 0}
        hardness_dict_erro[db_id] = {'easy':0, 'medium': 0, 'hard': 0, 'extra hard': 0}
    for gold_item, pred in tqdm(zip(gold_data, pred_data)):
        db_id = gold_item['db_id']
        gold = find_sql(gold_item['output'])
        question = gold_item['input'].replace('\n\n###Response:', '').replace('###Input:\n', '')
        # gold_item['instruction'] = gold_item['instruction'].replace('I want you to act as a SQL terminal in front of an example database, you need only to return the sql command to me.Below is an instruction that describes a task, Write a response that appropriately completes the request.\n\"\n##Instruction:\n', 
                                        #  'Below is an instruction that describes a task, Write a response that appropriately completes the request.\n ###Instruction:\n ##task:Given SQLite database schema information, a question about this database, and the corresponding SQL, your task is to check for any syntax errors or semantic errors in the SQL. If errors are found, please correct them to form the proper SQL, put corrected SQL inside the ```sql and ``` tags.\n ##database schema:\n')
        db_path = db_base_path +'/' + db_id +'/'
        sql = gold
        db = gold_item['db_id']
        db = os.path.join(db_base_path, db, db + ".sqlite")
        schema = Schema(get_schema(db))
        try:
            g_sql = get_sql(schema, sql)
        except Exception as e:
            continue
        hardness = eval_hardness(g_sql)
        try:
            if eval_exec_match(db_path, pred, gold, False, False, False) == 0:
                gold_item['pred'] = pred
                gold_item['gold'] = gold
                gold_item['hardness'] = hardness
                erro_data.append(gold_item)
                erro_db_id[db_id] = erro_db_id.get(db_id, 0) + 1
                hardness_dict_erro[db_id]['easy'] = hardness_dict_erro.get(db_id).get('easy') + 1
        except:
            gold_item['pred'] = pred
            gold_item['gold'] = gold
            gold_item['hardness'] = hardness
            erro_data.append(gold_item)
            erro_db_id[db_id] = erro_db_id.get(db_id, 0) + 1
            hardness_dict_erro[db_id]['easy'] = hardness_dict_erro.get(db_id).get('easy') + 1


        all_db_id[db_id] = all_db_id.get(db_id, 0) + 1
        

        if hardness == 'easy':
            easy_data.append(1)
            hardness_dict[db_id]['easy'] = hardness_dict.get(db_id).get('easy') + 1
            
        if hardness == 'medium':
            medium_data.append(1)
            hardness_dict[db_id]['medium'] = hardness_dict.get(db_id).get('medium') + 1
            
        if hardness == 'hard':
            hard_data.append(1)
            hardness_dict[db_id]['hard'] = hardness_dict.get(db_id).get('hard') + 1

        if hardness == 'extra hard':
            extra_data.append(1)
            hardness_dict[db_id]['extra hard'] = hardness_dict.get(db_id).get('extra hard') + 1
        hardness_dict[db_id]['len_prompt'] = len(gold_item['instruction'])
            
  
    print('easy_data', len(easy_data), len(easy_data)/len(erro_data))
    print('medium_data', len(medium_data), len(medium_data)/len(erro_data))
    print('hard_data', len(hard_data), len(hard_data)/len(erro_data))
    print('extra_data', len(extra_data), len(extra_data)/len(erro_data))

    print(len(erro_data))
    json.dump(erro_data, open(output_path, 'w'), indent=4, ensure_ascii=False)
    db_ans_result = []
    for key, value in all_db_id.items():
        db_ans_result.append({
            'db_id': key,
            'data_num': value,
            'erro_num': erro_db_id[key] if key in erro_db_id else 0,
            'erro_ratio': round(erro_db_id[key]/value, 4)*100 if key in erro_db_id else 0,
            'hardness': hardness_dict[key],
            'hardness_erro_ratio': {'easy_erro_ratio': round((hardness_dict[key]['easy']/hardness_dict[key]['easy']) if hardness_dict[key]['easy'] != 0 else -1, 4)*100,
                                    'medium_erro_ratio': round((hardness_dict[key]['medium']/hardness_dict[key]['medium']) if hardness_dict[key]['medium'] != 0 else -1, 4)*100,
                                    'hard_erro_ratio': round((hardness_dict[key]['hard']/hardness_dict[key]['hard']) if hardness_dict[key]['hard'] != 0 else -1, 4)*100,
                                    'extra_hard_erro_ratio': round((hardness_dict[key]['extra hard']/hardness_dict[key]['extra hard']) if hardness_dict[key]['extra hard'] != 0 else -1, 4)*100,
                                    }
        })
    db_ans_result = sorted(db_ans_result, key=lambda x: x['erro_ratio'], reverse=True)
    json.dump(db_ans_result, open(db_ans_path, 'w'), indent=4, ensure_ascii=False)

    # with open(db_ans_path, 'w') as f:
    #     for item in db_ans_result:
    #         db_id = item['db_id']
    #         data_num = item['data_num']
    #         erro_ratio = item['erro_ratio']
    #         erro_num = item['erro_num']
    #         f.write(f'db_id: {db_id}\tdata_num: {data_num}\terro_num: {erro_num}\terro_ratio: {erro_ratio} \n')






