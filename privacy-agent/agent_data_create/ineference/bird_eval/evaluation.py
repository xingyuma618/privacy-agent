import sys
import json
import argparse
import sqlite3
import multiprocessing as mp
import os
import re
from typing import Any, Dict, List, Optional, Tuple

try:
    from func_timeout import func_timeout, FunctionTimedOut
except Exception:
    # Fallback for environments where func_timeout is not installed.
    # Works on Unix-like systems, including most Linux training/eval servers.
    import signal

    class FunctionTimedOut(TimeoutError):
        pass

    def _timeout_handler(signum, frame):
        raise FunctionTimedOut()

    def func_timeout(timeout, func, args=(), kwargs=None):
        kwargs = kwargs or {}
        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, float(timeout))
        try:
            return func(*args, **kwargs)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)

# sqlglot is only required when you evaluate train data without precomputed difficulty.
# For BIRD dev/test with diff.json difficulty already provided, evaluation can run without it.
try:
    from sqlglot import parse_one  # type: ignore
    from eval_hardneess_sqlglot import eval_hardness  # type: ignore
except Exception:
    parse_one = None
    eval_hardness = None

BIRD_SEP = '\t----- bird -----\t'


def load_json(path: str) -> Any:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def dump_json(obj: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)


def _clean_sql_text(sql: Any) -> str:
    if sql is None:
        return ' '
    sql = str(sql)
    return sql.replace('```sql\n', '').replace('\n```', '').strip() or ' '


def _parse_sql_db_line(line: str) -> Tuple[str, str]:
    line = line.rstrip('\n')
    if not line.strip():
        return ' ', ''
    if BIRD_SEP in line:
        sql, db_name = line.split(BIRD_SEP, 1)
        return _clean_sql_text(sql), db_name.strip()
    # BIRD gold usually uses: SQL \t db_id. Use rsplit because SQL may contain tabs rarely.
    if '\t' in line:
        sql, db_name = line.rsplit('\t', 1)
        return _clean_sql_text(sql), db_name.strip()
    return _clean_sql_text(line), ''


def _db_path(db_root_path: str, db_name: str) -> str:
    return os.path.join(db_root_path, db_name, db_name + '.sqlite')


def _extract_bird_index(value: Any) -> Optional[int]:
    """Extract BIRD original integer index from values like 164, '164', 'bird_164', 'idx_164'."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if not s:
        return None
    if re.fullmatch(r'\d+', s):
        return int(s)
    m = re.fullmatch(r'(?:bird|idx)[_\-]?(\d+)', s, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def _resolve_original_idx_from_all_res(item: Dict[str, Any]) -> Optional[int]:
    """
    Priority is important:
    - masksql_idx=bird_xxx is the most reliable for MaskSQL subset evaluation.
    - question_id/case_id/eval_idx are reliable if agent writes original BIRD id.
    - idx is often only subset-local, so use it last.
    """
    for key in ('eval_original_idx', 'bird_idx', 'question_id', 'case_id', 'masksql_idx'):
        idx = _extract_bird_index(item.get(key))
        if idx is not None:
            return idx
    return _extract_bird_index(item.get('idx'))


def load_prediction_records(
    predicted_sql_path: str,
    db_root_path: str,
    output_dir: str = '',
    all_res_path: str = '',
) -> List[Dict[str, Any]]:
    """
    Load predict.json and preserve prediction order.

    Supported predict.json format:
        {"164": "SELECT ...\t----- bird -----\tfinancial", ...}

    For partial MaskSQL-subset evaluation, old agent versions may write subset-local keys
    0,1,2,... instead of original BIRD ids. In that case this function tries to recover
    original ids from output_dir/all_res.json, especially masksql_idx='bird_xxx'.
    """
    pred_obj = load_json(predicted_sql_path)
    if not isinstance(pred_obj, dict):
        raise ValueError(f'Expected predicted_sql_path to be a JSON dict, got {type(pred_obj)}: {predicted_sql_path}')

    records: List[Dict[str, Any]] = []
    for order_idx, (pred_key, sql_str) in enumerate(pred_obj.items()):
        if isinstance(sql_str, str):
            pred_sql, db_name = _parse_sql_db_line(sql_str)
        else:
            pred_sql, db_name = ' ', ''
        records.append({
            'run_idx': order_idx,
            'pred_key': pred_key,
            'pred_sql': pred_sql,
            'db_name': db_name,
            'db_path': _db_path(db_root_path, db_name) if db_name else '',
            'original_idx': _extract_bird_index(pred_key),
        })

    if not all_res_path and output_dir:
        candidate = os.path.join(output_dir, 'all_res.json')
        if os.path.exists(candidate):
            all_res_path = candidate

    if all_res_path and os.path.exists(all_res_path):
        try:
            all_res = load_json(all_res_path)
        except Exception:
            all_res = None
        if isinstance(all_res, list):
            for i, item in enumerate(all_res[:len(records)]):
                if not isinstance(item, dict):
                    continue
                idx_from_all_res = _resolve_original_idx_from_all_res(item)
                if idx_from_all_res is not None:
                    records[i]['original_idx'] = idx_from_all_res
                # If predict.json did not store db id, recover it from all_res.
                if not records[i].get('db_name') and item.get('db_id'):
                    records[i]['db_name'] = item['db_id']
                    records[i]['db_path'] = _db_path(db_root_path, item['db_id'])

    return records


def load_gold_records(ground_truth_path: str, db_root_path: str, data_mode: str = 'dev', diff_path: str = '') -> List[Dict[str, Any]]:
    """Load full BIRD gold SQL records; subset selection is done later by original_idx."""
    records: List[Dict[str, Any]] = []
    if data_mode == 'train' and diff_path and os.path.exists(diff_path):
        gold_json = load_json(diff_path)
        for i, item in enumerate(gold_json):
            sql = _clean_sql_text(item.get('output', ' '))
            db_name = item.get('db_id', '')
            records.append({'original_idx': i, 'sql': sql, 'db_name': db_name, 'db_path': _db_path(db_root_path, db_name)})
        return records

    with open(ground_truth_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            sql, db_name = _parse_sql_db_line(line)
            records.append({'original_idx': i, 'sql': sql, 'db_name': db_name, 'db_path': _db_path(db_root_path, db_name)})
    return records


def attach_gold_to_predictions(
    pred_records: List[Dict[str, Any]],
    gold_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Select the correct gold SQL by original BIRD index for partial evaluation."""
    n_gold = len(gold_records)
    selected: List[Dict[str, Any]] = []
    for rec in pred_records:
        original_idx = rec.get('original_idx')
        if original_idx is None:
            raise ValueError(
                f"Cannot resolve original BIRD index for prediction run_idx={rec.get('run_idx')}, "
                f"pred_key={rec.get('pred_key')}. Fix predict.json keys or provide all_res.json with masksql_idx/question_id."
            )
        if original_idx < 0 or original_idx >= n_gold:
            raise IndexError(
                f"Prediction original_idx={original_idx} is out of range for gold file with {n_gold} rows. "
                f"run_idx={rec.get('run_idx')}, pred_key={rec.get('pred_key')}"
            )
        gt = gold_records[original_idx]
        out = dict(rec)
        out['gt_sql'] = gt['sql']
        out['gt_db_name'] = gt['db_name']
        # Prefer prediction db path when available, otherwise use GT db path.
        if not out.get('db_path'):
            out['db_path'] = gt['db_path']
        selected.append(out)
    return selected


def execute_sql(predicted_sql: str, ground_truth: str, db_path: str) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(predicted_sql)
        predicted_res = cursor.fetchall()
        cursor.execute(ground_truth)
        ground_truth_res = cursor.fetchall()
        return int(set(predicted_res) == set(ground_truth_res))


def execute_model(record: Dict[str, Any], meta_time_out: float) -> Dict[str, Any]:
    try:
        res = func_timeout(
            meta_time_out,
            execute_sql,
            args=(record['pred_sql'], record['gt_sql'], record['db_path']),
        )
        error = ''
    except KeyboardInterrupt:
        sys.exit(0)
    except FunctionTimedOut:
        res = 0
        error = 'timeout'
    except Exception as e:
        res = 0
        error = f'{type(e).__name__}: {str(e)}'

    return {
        'sql_idx': record['run_idx'],
        'original_idx': record['original_idx'],
        'pred_key': record.get('pred_key'),
        'db_id': record.get('db_name') or record.get('gt_db_name'),
        'res': int(res),
        'error': error,
    }


def _execute_model_star(args: Tuple[Dict[str, Any], float]) -> Dict[str, Any]:
    return execute_model(*args)


def run_sqls_parallel(records: List[Dict[str, Any]], num_cpus: int = 1, meta_time_out: float = 30.0) -> List[Dict[str, Any]]:
    if not records:
        return []
    num_cpus = max(1, min(int(num_cpus), len(records)))
    if num_cpus == 1:
        return [execute_model(rec, meta_time_out) for rec in records]

    with mp.Pool(processes=num_cpus) as pool:
        results = pool.map(_execute_model_star, [(rec, meta_time_out) for rec in records])
    return results


def sort_results(list_of_dicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(list_of_dicts, key=lambda x: x['sql_idx'])


def _difficulty_for_content(content: Dict[str, Any], data_mode: str) -> str:
    if data_mode == 'train':
        if parse_one is None or eval_hardness is None:
            # Do not crash merely because sqlglot is absent. Put these samples into moderate.
            return content.get('difficulty', 'moderate') or 'moderate'
        sql = _clean_sql_text(content.get('output', ' '))
        try:
            parse_sql = parse_one(sql, dialect='sqlite')
            return eval_hardness(parse_sql)
        except Exception:
            return 'moderate'
    return content.get('difficulty', 'unknown') or 'unknown'


def compute_acc_by_diff(exec_results: List[Dict[str, Any]], diff_json_path: str, data_mode: str = 'dev'):
    num_queries = len(exec_results)
    if num_queries == 0:
        return 0.0, 0.0, 0.0, 0.0, [0, 0, 0, 0]

    contents = load_json(diff_json_path) if diff_json_path and os.path.exists(diff_json_path) else []
    grouped = {'simple': [], 'moderate': [], 'challenging': []}

    for res in exec_results:
        original_idx = res.get('original_idx')
        difficulty = 'unknown'
        if isinstance(original_idx, int) and 0 <= original_idx < len(contents):
            content = contents[original_idx]
            if isinstance(content, dict):
                difficulty = _difficulty_for_content(content, data_mode)
        res['difficulty'] = difficulty
        if difficulty in grouped:
            grouped[difficulty].append(res)

    def acc(items: List[Dict[str, Any]]) -> float:
        return (sum(x['res'] for x in items) / len(items) * 100) if items else 0.0

    simple_acc = acc(grouped['simple'])
    moderate_acc = acc(grouped['moderate'])
    challenging_acc = acc(grouped['challenging'])
    all_acc = sum(x['res'] for x in exec_results) / num_queries * 100
    count_lists = [len(grouped['simple']), len(grouped['moderate']), len(grouped['challenging']), num_queries]
    return simple_acc, moderate_acc, challenging_acc, all_acc, count_lists


def print_data(score_lists: List[float], count_lists: List[int], out_file: str) -> None:
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, 'a', encoding='utf-8') as f:
        print('\n' + out_file + '\n', file=f)
        levels = ['simple', 'moderate', 'challenging', 'total']
        print('{:20} {:20} {:20} {:20} {:20}'.format('', *levels), file=f)
        print('{:20} {:<20} {:<20} {:<20} {:<20}'.format('count', *count_lists), file=f)
        print('======================================    ACCURACY    =====================================', file=f)
        print('{:20} {:<20.2f} {:<20.2f} {:<20.2f} {:<20.2f}'.format('accuracy', *score_lists), file=f)


def write_success_fail_files(output_dir: str, exec_results: List[Dict[str, Any]], pred_records: List[Dict[str, Any]]) -> None:
    all_res_path = os.path.join(output_dir, 'all_res.json')
    if os.path.exists(all_res_path):
        try:
            all_res = load_json(all_res_path)
        except Exception:
            all_res = []
    else:
        all_res = []

    success_res, fail_res = [], []
    for res in exec_results:
        run_idx = res['sql_idx']
        if isinstance(all_res, list) and run_idx < len(all_res) and isinstance(all_res[run_idx], dict):
            item = dict(all_res[run_idx])
        else:
            pred = pred_records[run_idx]
            item = {
                'db_id': pred.get('db_name') or pred.get('gt_db_name'),
                'pred_sql': pred.get('pred_sql'),
                'gold_sql': pred.get('gt_sql'),
            }
        item['exec_acc'] = res['res']
        item['eval_original_idx'] = res.get('original_idx')
        item['eval_pred_key'] = res.get('pred_key')
        item['difficulty'] = res.get('difficulty', 'unknown')
        if res.get('error'):
            item['exec_error'] = res['error']
        if res['res'] == 1:
            success_res.append(item)
        else:
            fail_res.append(item)

    dump_json(success_res, os.path.join(output_dir, 'success_res.json'))
    dump_json(fail_res, os.path.join(output_dir, 'fail_res.json'))


def bird_eval_main(
    predicted_sql_path: str,
    ground_truth_path: str,
    db_root_path: str,
    output_dir: str,
    diff_json_path: str,
    data_mode: str,
    num_cpus: int,
    meta_time_out: float,
    all_res_path: str = '',
):
    pred_records = load_prediction_records(
        predicted_sql_path=predicted_sql_path,
        db_root_path=db_root_path,
        output_dir=output_dir,
        all_res_path=all_res_path,
    )
    gold_records = load_gold_records(
        ground_truth_path=ground_truth_path,
        db_root_path=db_root_path,
        data_mode=data_mode,
        diff_path=diff_json_path,
    )
    eval_records = attach_gold_to_predictions(pred_records, gold_records)

    print(f'Loaded predictions: {len(pred_records)}')
    print(f'Loaded gold records: {len(gold_records)}')
    print(f'Original indices example: {[r["original_idx"] for r in eval_records[:10]]}')

    exec_results = run_sqls_parallel(eval_records, num_cpus=num_cpus, meta_time_out=meta_time_out)
    exec_results = sort_results(exec_results)

    print('start calculate')
    simple_acc, moderate_acc, challenging_acc, acc, count_lists = compute_acc_by_diff(
        exec_results,
        diff_json_path,
        data_mode=data_mode,
    )
    score_lists = [simple_acc, moderate_acc, challenging_acc, acc]
    print_data(score_lists, count_lists, out_file=os.path.join(output_dir, 'eval.log'))
    write_success_fail_files(output_dir, exec_results, eval_records)
    dump_json(exec_results, os.path.join(output_dir, 'exec_result.json'))

    print('===========================================================================================')
    print('Finished evaluation')
    print(f'ACC: {acc:.2f}')
    return exec_results


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--predicted_sql_path', type=str, required=True, default='')
    args_parser.add_argument('--ground_truth_path', type=str, required=True, default='')
    args_parser.add_argument('--data_mode', type=str, required=True, default='dev')
    args_parser.add_argument('--db_root_path', type=str, required=True, default='')
    args_parser.add_argument('--num_cpus', type=int, default=1)
    args_parser.add_argument('--meta_time_out', type=float, default=30.0)
    args_parser.add_argument('--mode_gt', type=str, default='gt')        # kept for compatibility
    args_parser.add_argument('--mode_predict', type=str, default='gpt')  # kept for compatibility
    args_parser.add_argument('--difficulty', type=str, default='simple') # kept for compatibility
    args_parser.add_argument('--diff_json_path', type=str, default='')
    args_parser.add_argument('--output_dir', type=str, required=True, default='')
    args_parser.add_argument('--all_res_path', type=str, default='', help='Optional path to all_res.json for resolving subset original indices.')
    args = args_parser.parse_args()

    bird_eval_main(
        predicted_sql_path=args.predicted_sql_path,
        ground_truth_path=args.ground_truth_path,
        db_root_path=args.db_root_path,
        output_dir=args.output_dir,
        diff_json_path=args.diff_json_path,
        data_mode=args.data_mode,
        num_cpus=args.num_cpus,
        meta_time_out=args.meta_time_out,
        all_res_path=args.all_res_path,
    )
