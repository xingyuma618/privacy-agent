from sqlglot import parse_one, exp
import sqlglot

def get_outer_sql(parsed):
    p = parsed
    q = parsed
    while p.key != 'select':
        # print(p.key)
        p = q.this
        q = q.this
    return p 

def count_or(parsed):
    cnt_or = 0
    p = parsed.this
    while p.key == 'or' or p.key=='and':
        if p.key == 'or':
            cnt_or += 1
        p = p.this
    return cnt_or

def count_like(parsed):
    return len(parsed.find_all(exp.Like))

def count_cond(parsed):
    cnt_cond = 0
    p = parsed.this
    while p.key == 'or' or p.key=='and':
        cnt_cond += 1
        p = p.this
    return cnt_cond


def count_component1(parsed):
    count = 0 
    outer_sql_parsed = get_outer_sql(parsed)
    tbs = set()
    cnt_or = 0
    cnt_like = len(list(outer_sql_parsed.find_all(exp.Like)))

    for item in outer_sql_parsed.iter_expressions():
        #最外层SQL存在where条件
        if str(item).startswith('WHERE'):
            count += 1
            #计算or的数量
            cnt_or = count_or(item)
        #统计or的数量
        if str(item).startswith('FROM'):
            cnt_or = count_or(item)
        #统计or的数量
        if str(item).startswith('HAVING'):
            cnt_or = count_or(item)

        #最外层存在groupBy
        if str(item).startswith('GROUP BY'):
            count += 1
        #最外层存在order by
        if str(item).startswith('ORDER BY'):
            count += 1
        #最外层存在limit
        if str(item).startswith('LIMIT'):
            count += 1    
            
        #计算数据表的数量
        if str(item).startswith('FROM') or str(item).startswith('JOIN'):
            for tb in item.find_all(exp.Table):
                tbs.add(tb)
    
    count += cnt_or
    count += len(tbs) - 1
    count += cnt_like
    return count
    

def count_component2(parse_sql):
    return len(list(parse_sql.find_all(exp.Select))) - 1


def count_others(parse_sql):
    count = 0
    cnt_cond = 0
    cnt_group = 0
    outer_sql_parsed = get_outer_sql(parse_sql)
    agg_count = len(list(outer_sql_parsed.find_all(exp.AggFunc)))
    
    #聚合函数的数量
    if agg_count > 1:
        count += 1
    #统计列名
    if len(outer_sql_parsed.expressions) > 1:
        count += 1
    for item in outer_sql_parsed.iter_expressions():
        #where中查询条件的数量
        if str(item).startswith('WHERE'):
            cnt_cond = count_cond(item)
        #grou by 中列名的数量
        if str(item).startswith('GROUP BY'):
            cnt_group = len(item.expressions)




    if cnt_cond > 1:
        count += 1
    if cnt_group > 1:
        count += 1

    return count


# def count_others(sql):
#     print(sql)
#     count = 0
#     # number of aggregation
#     #统计select, where, groupby, orderby, having中的聚合函数数量，如果大于1，则count++
#     agg_count = count_agg(sql['select'][1])
#     agg_count += count_agg(sql['where'][::2])
#     agg_count += count_agg(sql['groupBy'])
#     if len(sql['orderBy']) > 0:
#         agg_count += count_agg([unit[1] for unit in sql['orderBy'][1] if unit[1]] +
#                             [unit[2] for unit in sql['orderBy'][1] if unit[2]])
#     agg_count += count_agg(sql['having'])

#     if agg_count > 1:
#         count += 1
#     # print(sql['select'][1])
#     # number of select columns
#     #列名数量
#     if len(sql['select'][1]) > 1:
#         count += 1

#     # number of where conditions
#     #条件数量
#     if len(sql['where']) > 1:
#         count += 1

#     # number of group by clauses
    
#     if len(sql['groupBy']) > 1:
#         count += 1

#     return count

def eval_hardness(parse_sql):
    count_comp1_ = count_component1(parse_sql)
    # print(count_comp1_)
    count_comp2_ = count_component2(parse_sql)
    # print(count_comp2_)
    count_others_ = count_others(parse_sql)
    # print(count_others_)

    if count_comp1_ <= 1 and count_others_ == 0 and count_comp2_ == 0:
        return "easy"
    elif (count_others_ <= 2 and count_comp1_ <= 1 and count_comp2_ == 0) or \
            (count_comp1_ <= 2 and count_others_ < 2 and count_comp2_ == 0):
        return "medium"
    elif (count_others_ > 2 and count_comp1_ <= 2 and count_comp2_ == 0) or \
            (2 < count_comp1_ <= 3 and count_others_ <= 2 and count_comp2_ == 0) or \
            (count_comp1_ <= 1 and count_others_ == 0 and count_comp2_ <= 1):
        return "hard"
    else:
        return "extra hard"
    
if __name__ == '__main__':
    import json
    from tqdm import tqdm
    # sql = "SELECT T1.department_id, T3.born_state, count(*) FROM department AS T1 JOIN management AS T2 ON T1.department_id  =  T2.department_id JOIN head AS T3 ON T2.head_id  =  T3.head_id WHERE T1.name  =  'Treasury' INTERSECT SELECT T3.born_state FROM department AS T1 JOIN management AS T2 ON T1.department_id  =  T2.department_id JOIN head AS T3 ON T2.head_id  =  T3.head_id WHERE T1.name  =  'Homeland Security'"
    data = json.load(open('/home/maxingyu/work/docker/DB-GPT-Hub/dbgpt_hub/data/bird/bird_train.json', 'r'))
    easy = []
    medium = []
    hard = []
    extra_hard = []
    for item in tqdm(data):
        sql = item['output']
        try:
            parse_sql = parse_one(sql, dialect='sqlite')
        except:
            print(sql)
            continue

        hardness = eval_hardness(parse_sql)
        
        if 'easy' == hardness:
            easy.append(item)
            # print(sql)
            # break
        if 'medium' == hardness:
            medium.append(item)
            # print(sql)
            # break
        if 'hard' == hardness:
            hard.append(item)
            # print(sql)
            # break
        if 'extra hard' == hardness:
            extra_hard.append(item)
            # print(sql)
            # break
    print(len(easy))
    print(len(medium))
    print(len(hard))
    print(len(extra_hard))

