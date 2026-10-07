"""Sequential single-replica benchmark with reproducible inputs, fixed decode length."""
import argparse,asyncio,json,time,statistics
from pathlib import Path
import httpx
ROOT=Path(__file__).resolve().parents[1]
async def run(endpoint,concurrency,repeat,tokens,n,warm):
 sample=json.loads((ROOT/'data/bird_train.jsonl').open().readline())
 prompt='Schema:\n'+(sample['db_schema']+'\n')*repeat+'\nExplain the schema and generate many varied SQLite SELECT examples. Keep writing until the output limit.'
 results=[];errors=[];sem=asyncio.Semaphore(concurrency)
 async with httpx.AsyncClient(trust_env=False,timeout=900,limits=httpx.Limits(max_connections=256,max_keepalive_connections=128)) as client:
  async def request(i):
   async with sem:
    started=time.monotonic()
    try:
     r=await client.post(endpoint+'/chat/completions',json={'model':'Qwen3.8-27B','messages':[{'role':'user','content':prompt}],'max_tokens':tokens,'temperature':0,'seed':2026+i,'ignore_eos':True,'chat_template_kwargs':{'enable_thinking':False}});r.raise_for_status();d=r.json()
     results.append({'seconds':time.monotonic()-started,'input_tokens':d['usage']['prompt_tokens'],'output_tokens':d['usage']['completion_tokens']})
    except Exception as e:errors.append(str(e))
  if warm:
   await asyncio.gather(*(request(i) for i in range(concurrency)));results.clear();errors.clear()
  started=time.monotonic();await asyncio.gather(*(request(i) for i in range(n)));elapsed=time.monotonic()-started
 return {'endpoint':endpoint,'concurrency':concurrency,'input_repeat':repeat,'output_limit':tokens,'requests':n,'successes':len(results),'seconds':elapsed,'output_tokens_per_second':sum(r['output_tokens'] for r in results)/elapsed,'mean_request_seconds':statistics.mean(r['seconds'] for r in results) if results else None,'mean_input_tokens':statistics.mean(r['input_tokens'] for r in results) if results else None,'errors':errors}
p=argparse.ArgumentParser();p.add_argument('--endpoint',required=True);p.add_argument('--backend',required=True);p.add_argument('--concurrency',nargs='+',type=int,default=[8,25,48]);p.add_argument('--tokens',type=int,default=512);p.add_argument('--repeat',type=int,default=6);p.add_argument('--requests',type=int,default=96);args=p.parse_args();results=[]
for c in args.concurrency:
 result=asyncio.run(run(args.endpoint,c,args.repeat,args.tokens,args.requests,True));results.append(result);print(json.dumps(result),flush=True)
 (ROOT/'runs'/f'benchmark_backend_{args.backend}.json').write_text(json.dumps(results,indent=2)+'\n')
