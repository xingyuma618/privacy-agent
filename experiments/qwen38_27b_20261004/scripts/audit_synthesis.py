"""Read-only audit of a fixed cache snapshot; inference continues unchanged."""
import collections,concurrent.futures,json,statistics,sys,time,re,copy
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from common import atomic_json,read_jsonl,stable_hash
from database import execute,execution_match,parse_query
from masking import mask,unmask,contains
from search import sql_from_answer
from prompts import TOOLS
stamp=time.strftime('%Y%m%d_%H%M%S');out=ROOT/'runs'/('audit_'+stamp);out.mkdir()
run=ROOT/'runs/full_parallel100';snapshot={'captured_unix':time.time(),'methods':{},'labels':{},'exports':{}}
for method in ['prompt','mcts']:
 snapshot['methods'][method]=[json.loads(p.read_text()) for p in sorted((run/method/'cases').glob('*.json'))]
 export=run/method/'train.jsonl';snapshot['exports'][method]=read_jsonl(export) if export.exists() else []
for p in (run/'labels').glob('*.json'):snapshot['labels'][p.stem]=json.loads(p.read_text())
atomic_json(out/'snapshot.json',snapshot)
items={x['case_key']:x for x in read_jsonl(ROOT/'data/bird_train.jsonl')};required={t['function']['name']:t['function']['parameters'].get('required',[]) for t in TOOLS}
summary={'snapshot_unix':snapshot['captured_unix'],'methods':{},'label_errors':{},'issues':[],'exports':{},'inputs_database_count':len({i['db_id'] for i in items.values()})};accepted=[]
for method,rows in snapshot['methods'].items():
 counters=collections.Counter();examples=collections.defaultdict(list);bydb=collections.defaultdict(collections.Counter)
 for row in rows:
  counters['status_'+row['status']]+=1
  if row['status']=='infrastructure_error':summary['label_errors'][row['case_key']]=row.get('error','');continue
  bydb[row['db_id']][row['status']]+=1
  if row['status']!='accepted':counters['rejection_'+row['evaluation']['reason']]+=1;continue
  accepted.append((method,row));key=row['case_key'];issues=[];pending={};masking=None;last_sql_response=None;last_unmask_result=None;checked_sql=[];outbound=[]
  if not row['evaluation'].get('execution_correct') or row['evaluation'].get('privacy_score')!=1 or row['evaluation'].get('direct_leaks'):issues.append('inconsistent_acceptance_metadata')
  for m in row['messages']:
   if m['role']=='assistant':
    if pending:issues.append('missing_tool_response')
    for call in m.get('tool_calls',[]):
     fn=call['function'];name=fn['name'];pending[call['id']]=call
     try:args=json.loads(fn['arguments']);assert isinstance(args,dict)
     except Exception:issues.append('invalid_tool_arguments');continue
     if name not in required:issues.append('unknown_tool')
     elif any(k not in args for k in required[name]):issues.append('missing_required_arguments')
     if name=='sql_generate':
      outbound.append({'masked_query':args.get('masked_query',''),'masked_schema_str':args.get('masked_schema_str','')})
      if masking:
       if args.get('masked_query')!=masking['masked_query']:issues.append('outbound_query_differs_from_mask_tool')
       if args.get('masked_schema_str')!=masking['masked_schema_str']:issues.append('outbound_schema_differs_from_mask_tool')
      else:issues.append('outbound_without_mask_tool')
     if name=='sql_unmask':
      if masking and args.get('unmask_dict')!=masking['unmask_dict']:issues.append('provided_unmask_map_differs_from_canonical')
      if last_sql_response:
       try:
        if parse_query(sql_from_answer(args['masked_sql'])).sql()!=parse_query(sql_from_answer(last_sql_response)).sql():issues.append('masked_sql_edited_before_unmask')
       except Exception:pass
     if name=='execute_sql_check':checked_sql.append(args.get('unmasked_sql',''))
   elif m['role']=='tool':
    call=pending.pop(m.get('tool_call_id'),None)
    if not call:issues.append('orphan_tool_response');continue
    name=call['function']['name'];args=json.loads(call['function']['arguments']);content=m['content']
    try:result=json.loads(content)
    except Exception:result=content
    if isinstance(result,dict) and result.get('error'):continue
    if name=='apply_typed_masking':
     try:
      expected=mask(args['query'],args['entities'],items[key]['structured_db_schema'])
      if expected!=result:issues.append('mask_tool_result_mismatch')
      masking=result
     except Exception as e:issues.append('mask_replay_error:'+str(e))
    if name=='sql_generate':last_sql_response=content
    if name=='sql_unmask':
     last_unmask_result=content
     try:
      expected=unmask(sql_from_answer(args['masked_sql']),masking['unmask_dict'])
      if expected!=content:issues.append('unmask_tool_result_mismatch')
     except Exception as e:issues.append('unmask_replay_error:'+str(e))
  if pending:issues.append('missing_tool_response_at_end')
  if outbound!=row['outbound']:issues.append('outbound_log_mismatch')
  try:
   final=parse_query(row['pred_sql']).sql()
   if not any(parse_query(s).sql()==final for s in checked_sql):issues.append('final_sql_not_checked_in_trajectory')
   if last_unmask_result and parse_query(last_unmask_result).sql()!=final:issues.append('final_sql_edited_after_unmask')
  except Exception:issues.append('sql_provenance_parse_error')
  if row['evaluation'].get('errors'):issues.append('accepted_with_recovered_tool_errors')
  for issue in set(issues):counters[issue]+=1;examples[issue].append(key)
  if issues:summary['issues'].append({'method':method,'case_key':key,'issues':sorted(set(issues))})
 summary['methods'][method]={'counts':dict(counters),'examples':{k:v[:12] for k,v in examples.items()},'database_distribution':{k:dict(v) for k,v in bydb.items()}}
# Independent read-only SQL rerun for every accepted result in the snapshot.
def check(pair):
 method,row=pair;item=items[row['case_key']]
 try:
  p,pc=execute(item['db_path'],row['pred_sql']);g,gc=execute(item['db_path'],item['gold_sql'])
  correct=execution_match(item['db_path'],row['pred_sql'],item['gold_sql'])
  return {'method':method,'case_key':row['case_key'],'correct':correct,'gold_rows':len(g),'pred_rows':len(p),'gold_cols':gc,'pred_cols':pc}
 except Exception as e:return {'method':method,'case_key':row['case_key'],'error':str(e)}
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:checks=list(pool.map(check,accepted))
atomic_json(out/'execution_checks.json',checks)
for method in ['prompt','mcts']:
 rows=[r for r in checks if r['method']==method];summary['methods'][method]['independent_execution']={'tested':len(rows),'passed':sum(r.get('correct') is True for r in rows),'errors':[r for r in rows if r.get('error')],'mismatches':[r for r in rows if r.get('correct') is False],'empty_gold_cases':[r['case_key'] for r in rows if r.get('gold_rows')==0]}
# Exact student-tokenizer chat lengths for the CURRENT exported training rows.
from transformers import AutoTokenizer
tokenizer=AutoTokenizer.from_pretrained('/mnt/wh_intern/MXY/models/Qwen3-4B-Instruct-2507',local_files_only=True)
for method,rows in snapshot['exports'].items():
 lengths=[];fail=[];duplicates=collections.Counter(stable_hash(r) for r in rows)
 for i,row in enumerate(rows):
  try:lengths.append(len(tokenizer.apply_chat_template(row['messages'],tools=json.loads(row['tools']),tokenize=True,add_generation_prompt=False)))
  except Exception as e:fail.append({'row':i,'error':str(e)})
 summary['exports'][method]={'rows':len(rows),'chat_template_errors':fail,'duplicate_rows':sum(n-1 for n in duplicates.values()),'token_length_min':min(lengths) if lengths else None,'token_length_median':statistics.median(lengths) if lengths else None,'token_length_max':max(lengths) if lengths else None,'over_8192':sum(n>8192 for n in lengths),'over_16384':sum(n>16384 for n in lengths),'over_32768':sum(n>32768 for n in lengths)}
 atomic_json(out/(method+'_export_lengths.json'),lengths)
summary['unique_annotation_errors']=len(summary['label_errors']);summary['annotation_error_distribution']=dict(collections.Counter(summary['label_errors'].values()))
atomic_json(out/'summary.json',summary)
print(str(out),flush=True)
for method,s in summary['methods'].items():print(method,json.dumps(s['counts']),json.dumps(s['independent_execution']),flush=True)
print('EXPORTS',json.dumps(summary['exports']),flush=True)
