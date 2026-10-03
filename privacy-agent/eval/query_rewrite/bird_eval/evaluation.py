import sys
import json
import argparse
import sqlite3
import multiprocessing as mp
from func_timeout import func_timeout, FunctionTimedOut
from sqlglot import parse_one, exp
from eval_hardneess_sqlglot import eval_hardness
import os 
def load_json(dir):
    with open(dir, 'r') as j:
        contents = json.loads(j.read())
    return contents

def result_callback(result):
    exec_result.append(result)


def execute_sql(predicted_sql,ground_truth, db_path):
    # print(db_path)
    # conn = sqlite3.connect(db_path)
    with sqlite3.connect(db_path) as conn:
        # Connect to the database
        cursor = conn.cursor()
        cursor.execute(predicted_sql)
        predicted_res = cursor.fetchall()
        cursor.execute(ground_truth)
        ground_truth_res = cursor.fetchall()
        res = 0
        if set(predicted_res) == set(ground_truth_res):
            res = 1
        return res


def execute_model(predicted_sql,ground_truth, db_place, idx, meta_time_out):
    try:
        res = func_timeout(meta_time_out, execute_sql,
                                  args=(predicted_sql, ground_truth, db_place))
    except KeyboardInterrupt:
        sys.exit(0)
    except FunctionTimedOut:
        result = [(f'timeout',)]
        res = 0
    except Exception as e:
        # print(e)
        result = [(f'error',)]  # possibly len(query) > 512 or not executable
        res = 0
    # print(result)
    # result = str(set([ret[0] for ret in result]))
    result = {'sql_idx': idx, 'res': res}
    # print(result)
    return result

def run_sqls_parallel(sqls, db_places, num_cpus=1, meta_time_out=30.0):
    pool = mp.Pool(processes=num_cpus)
    results = []
    for i, sql_pair in enumerate(sqls):
        predicted_sql, ground_truth = sql_pair
        results.append(pool.apply_async(
            execute_model,
            args=(predicted_sql, ground_truth, db_places[i], i, meta_time_out)
        ))
    pool.close()
    pool.join()
    exec_result = [r.get() for r in results]
    return exec_result

def package_sqls(sql_path, db_root_path, mode='gpt', data_mode='dev', diff_path=''):
    clean_sqls = []
    db_path_list = []
    if mode == 'gpt':
        sql_data = json.load(open(sql_path, 'r'))
        # sql_data = open(sql_path).read().splitlines()
        for idx, sql_str in sql_data.items():
            if type(sql_str) == str:
                sql, db_name = sql_str.split('\t----- bird -----\t')
            else:
                sql, db_name = " ", "financial"
            clean_sqls.append(sql)
            db_path_list.append(os.path.join(db_root_path, db_name,  db_name + '.sqlite'))

    elif mode == 'gt':
        if 'train' in diff_path:
            gold_json = json.load(open(diff_path, 'r'))
            for item in gold_json:
                sql = item['output'].replace('```sql\n', '').replace('\n```', '')
                db_name = item['db_id']
                clean_sqls.append(sql)
                db_path_list.append(os.path.join(db_root_path, db_name,  db_name + '.sqlite'))
        else:
            sqls = open(sql_path)
            sql_txt = sqls.readlines()
            for idx, sql_str in enumerate(sql_txt):
                sql, db_name = sql_str.strip().split('\t')
                clean_sqls.append(sql)
                db_path_list.append(os.path.join(db_root_path, db_name,  db_name + '.sqlite'))

    return clean_sqls, db_path_list

def run_sqls_parallel(sqls, db_places, num_cpus=1, meta_time_out=30.0):
    pool = mp.Pool(processes=num_cpus)
    for i,sql_pair in enumerate(sqls):
        
        predicted_sql, ground_truth = sql_pair

        pool.apply_async(execute_model, args=(predicted_sql, ground_truth, db_places[i], i, meta_time_out), callback=result_callback)
    pool.close()
    pool.join()

# def run_sqls_parallel(sqls, db_places, num_cpus=1, meta_time_out=30.0):
#     pool = mp.Pool(processes=num_cpus)
#     results = []
#     for i, sql_pair in enumerate(sqls):
#         predicted_sql, ground_truth = sql_pair
#         results.append(pool.apply_async(
#             execute_model,
#             args=(predicted_sql, ground_truth, db_places[i], i, meta_time_out)
#         ))
#     pool.close()
#     pool.join()
#     exec_result = [r.get() for r in results]
#     return exec_result

def sort_results(list_of_dicts):
  return sorted(list_of_dicts, key=lambda x: x['sql_idx'])

def compute_acc_by_diff(exec_results,diff_json_path, data_mode='dev'):
    num_queries = len(exec_results)
    results = [res['res'] for res in exec_results]
    contents = load_json(diff_json_path)
    simple_results, moderate_results, challenging_results = [], [], []

    for i,content in enumerate(contents):
        if data_mode == 'train':
            sql = content['output'].replace('```sql\n', '').replace('\n```', '')
            try:
                parse_sql = parse_one(sql, dialect='sqlite')
                content['difficulty'] = eval_hardness(parse_sql)
            except:
                print(sql) 
                content['difficulty'] = 'moderate'

        if content['difficulty'] == 'simple':
            simple_results.append(exec_results[i])

        if content['difficulty'] == 'moderate':
            moderate_results.append(exec_results[i])

        if content['difficulty'] == 'challenging':
            challenging_results.append(exec_results[i])

    simple_acc = sum([res['res'] for res in simple_results])/len(simple_results)
    moderate_acc = sum([res['res'] for res in moderate_results])/len(moderate_results)
    challenging_acc = sum([res['res'] for res in challenging_results])/len(challenging_results)
    all_acc = sum(results)/num_queries
    count_lists = [len(simple_results), len(moderate_results), len(challenging_results), num_queries]
    return simple_acc * 100, moderate_acc * 100, challenging_acc * 100, all_acc * 100, count_lists



def print_data(score_lists,count_lists, out_file):
    with open(out_file, 'a') as f:
        print('\n' + out_file + '\n',  file=f)
        levels = ['simple', 'moderate', 'challenging', 'total']
        print("{:20} {:20} {:20} {:20} {:20}".format("", *levels), file=f)
        print("{:20} {:<20} {:<20} {:<20} {:<20}".format('count', *count_lists),  file=f)

        print('======================================    ACCURACY    =====================================',  file=f)
        print("{:20} {:<20.2f} {:<20.2f} {:<20.2f} {:<20.2f}".format('accuracy', *score_lists),  file=f)

def bird_eval_main(pred_queries, gt_queries, db_paths, num_cpus, meta_time_out, output_path, diff_json_path, data_mode):
    
    query_pairs = list(zip(pred_queries,gt_queries))
    exec_result = run_sqls_parallel(query_pairs, db_places=db_paths, num_cpus=num_cpus, meta_time_out=meta_time_out)
    exec_result = sort_results(exec_result)
    
    print('start calculate')
    simple_acc, moderate_acc, challenging_acc, acc, count_lists = \
        compute_acc_by_diff(exec_result,diff_json_path, data_mode=data_mode)
    score_lists = [simple_acc, moderate_acc, challenging_acc, acc]
    print_data(score_lists,count_lists, output_path)
    print('===========================================================================================')
    print("Finished evaluation")
    return exec_result

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--predicted_sql_path', type=str, required=True, default='')
    args_parser.add_argument('--ground_truth_path', type=str, required=True, default='')
    args_parser.add_argument('--data_mode', type=str, required=True, default='dev')
    args_parser.add_argument('--db_root_path', type=str, required=True, default='')
    args_parser.add_argument('--num_cpus', type=int, default=1)
    args_parser.add_argument('--meta_time_out', type=float, default=30.0)
    args_parser.add_argument('--mode_gt', type=str, default='gt')
    args_parser.add_argument('--mode_predict', type=str, default='gt')
    args_parser.add_argument('--difficulty',type=str,default='simple')
    args_parser.add_argument('--diff_json_path',type=str,default='')
    args_parser.add_argument('--output_dir',type=str,default='')
    args = args_parser.parse_args()
    exec_result = []

    pred_queries, db_paths = package_sqls(args.predicted_sql_path, args.db_root_path, mode=args.mode_predict,
                                          data_mode=args.data_mode)
    # generate gt sqls:
    gt_queries, db_paths_gt = package_sqls(args.ground_truth_path, args.db_root_path, mode='gt',
                                           data_mode=args.data_mode, diff_path=args.diff_json_path)
   

    query_pairs = list(zip(pred_queries,gt_queries))
    run_sqls_parallel(query_pairs, db_places=db_paths, num_cpus=args.num_cpus, meta_time_out=args.meta_time_out)
    exec_result = sort_results(exec_result)

    # all_res = json.load(open(f'{args.output_dir}/all_res.json', 'r'))
    # success_res, fail_res = [], []
    # for res, item in zip(all_res, exec_result):
    #     if item['res'] == 1:
    #         success_res.append(res)
    #     else:
    #         fail_res.append(res)

    # json.dump(success_res, open(f'{args.output_dir}/success_res.json', 'w'), ensure_ascii=False, indent=4)
    # json.dump(fail_res, open(f'{args.output_dir}/fail_res.json', 'w'), ensure_ascii=False, indent=4)

    print('start calculate')
    simple_acc, moderate_acc, challenging_acc, acc, count_lists = \
        compute_acc_by_diff(exec_result,args.diff_json_path, data_mode=args.data_mode)
    score_lists = [simple_acc, moderate_acc, challenging_acc, acc]
    print_data(score_lists,count_lists, out_file=f'{args.output_dir}/eval.log')
    print('===========================================================================================')
    print("Finished evaluation")
    