from __future__ import annotations
import argparse
import concurrent.futures
import copy
import json
import time
import traceback
from pathlib import Path
from common import ROOT, atomic_json, stable_hash, read_jsonl
from teacher import Teacher
from search import Environment, LABEL_INSTRUCTION
from masking import normalize_entities
from prompts import TOOLS

def fingerprint(config):
    return stable_hash({'config':config,'inputs':json.loads((ROOT/'data/input_manifest.json').read_text()),'code':{p.name:p.read_text() for p in sorted((ROOT/'src').glob('*.py'))}})

def export_all(run_dir, methods):
    summaries={}
    for method in methods:
        cache=run_dir/method/'cases';records=[]
        if cache.exists():
            for p in cache.glob('*.json'):
                records.append(json.loads(p.read_text()))
        records.sort(key=lambda r:r['index']);accepted=[r for r in records if r['status']=='accepted']
        path=run_dir/method/'train.jsonl';path.parent.mkdir(parents=True,exist_ok=True)
        temporary=path.with_suffix('.jsonl.tmp')
        with temporary.open('w') as file:
            for row in accepted:
                messages=[]
                for msg in row['messages']:
                    msg=copy.deepcopy(msg)
                    if msg.get('tool_calls'):
                        content=msg.get('content') or ''
                        for call in msg.pop('tool_calls'):
                            fn=call['function'];args=json.loads(fn['arguments'])
                            content+='\n<tool_call>\n'+json.dumps({'name':fn['name'],'arguments':args},ensure_ascii=False)+'\n</tool_call>'
                        msg['content']=content.strip()
                    messages.append(msg)
                file.write(json.dumps({'messages':messages,'tools':json.dumps(TOOLS,ensure_ascii=False)},ensure_ascii=False)+'\n')
        temporary.replace(path)
        summaries[method]={'completed':len(records),'accepted':len(accepted),'rejected':sum(r['status']=='rejected' for r in records),'infrastructure_errors':sum(r['status']=='infrastructure_error' for r in records),'seconds':sum(r.get('seconds',0) for r in records),'prompt_tokens':sum(c.get('prompt_tokens') or 0 for r in records for c in r.get('calls',[])),'completion_tokens':sum(c.get('completion_tokens') or 0 for r in records for c in r.get('calls',[]))}
    labels=list((run_dir/'labels').glob('*.json')) if (run_dir/'labels').exists() else []
    with (run_dir/'entity_extract_train.jsonl').open('w') as entities, (run_dir/'query_rewrite_train.jsonl').open('w') as rewrites:
        for p in sorted(labels):
            d=json.loads(p.read_text())
            entities.write(json.dumps({'messages':[{'role':'user','content':d['entity_input']},{'role':'assistant','content':json.dumps(d['entities'],ensure_ascii=False)}]},ensure_ascii=False)+'\n')
            if d.get('rewrite_equivalent'):
                rewrites.write(json.dumps({'messages':[{'role':'user','content':d['rewrite_input']},{'role':'assistant','content':d['rewrite']}]},ensure_ascii=False)+'\n')
    atomic_json(run_dir/'summary.json',{'updated_unix':time.time(),'methods':summaries,'label_cases':len(labels),'note':'Prompt and MCTS have identical inputs/acceptance criteria, but generation budgets differ; actual token costs are logged.'})
    return summaries

def process_case(item, config, run_dir, methods, identity):
    key=item['case_key'];label_path=run_dir/'labels'/(key+'.json');teacher=Teacher(config,key+'/labels')
    try:
        if label_path.exists():
            labels=json.loads(label_path.read_text())
            if labels['fingerprint']!=identity:raise ValueError('Stale label cache fingerprint.')
        else:
            query=item['question']+('\nEvidence: '+item['evidence'] if item.get('evidence') else '')
            prompt=LABEL_INSTRUCTION+'\nSchema:\n'+item['db_schema']+'\nQuestion/evidence:\n'+query+'\nGold SQL (annotation only):\n'+item['gold_sql']
            data=teacher.json('annotation',prompt);entities=normalize_entities(data,item['structured_db_schema'],query)
            rewrite_input='Schema:\n'+item['db_schema']+'\nQuestion:\n'+query
            action=teacher.call('rewrite',[{'role':'user','content':'Rewrite this question to make its database entities and relationships explicit. Preserve the meaning, conditions, literal values and evidence. Return only the rewritten question.\n'+rewrite_input}])[0]
            if action.get('_truncated'):raise ValueError('Teacher rewrite truncated.')
            rewrite=action['content'].strip()
            validation=teacher.json('judge','Do these two database questions express the same query intent, values, conditions, aggregation and relationships? Return {"equivalent": true or false, "reason": "short explanation"}.\nSchema:\n'+item['db_schema']+'\nGold SQL:\n'+item['gold_sql']+'\nOriginal:\n'+query+'\nRewrite:\n'+rewrite)
            labels={'fingerprint':identity,'case_key':key,'entities':entities,'entity_input':'- Database Schema:\n'+item['db_schema']+'\n- User Question:\n'+query,'rewrite_input':rewrite_input,'rewrite':rewrite,'rewrite_equivalent':validation.get('equivalent') is True,'rewrite_judgement':validation,'calls':teacher.calls}
            atomic_json(label_path,labels)
    except Exception as e:
        for method in methods:
            dest=run_dir/method/'cases'/(key+'.json')
            if not dest.exists():atomic_json(dest,{'fingerprint':identity,'index':item['index'],'case_key':key,'status':'infrastructure_error','error':'annotation: '+str(e),'calls':teacher.calls})
        return key
    for method in methods:
        dest=run_dir/method/'cases'/(key+'.json')
        if dest.exists():
            previous=json.loads(dest.read_text())
            if previous['fingerprint']!=identity:raise ValueError('Stale method cache fingerprint.')
            if previous['status']!='infrastructure_error':continue
        started=time.monotonic();model=Teacher(config,key+'/'+method);env=Environment(item,labels['entities'],model,config)
        result={'fingerprint':identity,'index':item['index'],'question_id':item['question_id'],'db_id':item['db_id'],'case_key':key,'method':method}
        try:
            state,evaluation=env.prompt() if method=='prompt' else env.mcts()
            status='accepted' if evaluation['accepted'] else 'rejected'
            if status!='accepted' and any(c.get('error') for c in model.calls):status='infrastructure_error'
            result.update(status=status,evaluation=evaluation,messages=state.messages,outbound=state.outbound,pred_sql=state.final_sql,stats=env.stats)
        except Exception as e:
            result.update(status='infrastructure_error',error=str(e),traceback=traceback.format_exc(),stats=env.stats)
        result.update(seconds=time.monotonic()-started,calls=model.calls)
        atomic_json(dest,result)
    return key

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,default=ROOT/'configs/synthesis.json');parser.add_argument('--run',default='full');parser.add_argument('--limit',type=int);parser.add_argument('--workers',type=int);parser.add_argument('--methods',nargs='+',choices=['prompt','mcts']);parser.add_argument('--retry-errors',action='store_true');args=parser.parse_args()
    config=json.loads(args.config.read_text());methods=args.methods or config['methods'];items=read_jsonl(ROOT/'data/bird_train.jsonl');items=items[:args.limit] if args.limit else items
    run_dir=ROOT/'runs'/args.run;run_dir.mkdir(parents=True,exist_ok=True);identity=fingerprint(config)
    manifest={'fingerprint':identity,'config':config,'methods':methods,'requested_cases':len(items),'started_unix':time.time(),'teacher_roles':['annotation','rewrite','extract','policy','sql','judge']}
    previous=run_dir/'manifest.json'
    if previous.exists() and json.loads(previous.read_text())['fingerprint']!=identity:raise SystemExit('Config/code/input changed: create a new run directory instead of reusing this cache.')
    atomic_json(previous,manifest)
    workers=args.workers or config['workers'];done=0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures={pool.submit(process_case,item,config,run_dir,methods,identity):item for item in items}
        for future in concurrent.futures.as_completed(futures):
            item=futures[future]
            try:key=future.result()
            except Exception as e:print(json.dumps({'case_key':item['case_key'],'error':str(e)}),flush=True);continue
            done+=1
            print(json.dumps({'case_completed':key,'done':done,'total':len(items)}),flush=True)
            if done==1 or done%100==0 or done==len(items):
                summary=export_all(run_dir,methods);print(json.dumps({'done':done,'total':len(items),'methods':summary},ensure_ascii=False),flush=True)
    export_all(run_dir,methods)
    print('Run complete: '+str(run_dir),flush=True)

if __name__=='__main__':main()
