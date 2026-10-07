"""Fixed-length, four-replica throughput benchmark; outputs are never training data."""
import argparse,asyncio,json,time,statistics
from pathlib import Path
import httpx
ROOT=Path(__file__).resolve().parents[1]
async def benchmark(concurrency,tokens,input_repeat):
    endpoints=json.loads((ROOT/'configs/synthesis.json').read_text())['endpoints']
    sample=json.loads((ROOT/'data/bird_train.jsonl').open().readline())
    prompt='Schema:\n'+(sample['db_schema']+'\n')*input_repeat+'\nExplain the schema and generate many varied SQLite SELECT examples. Keep writing until the output limit.'
    sem=asyncio.Semaphore(concurrency);latencies=[];errors=[];out_tokens=0;prompt_tokens=0
    async with httpx.AsyncClient(trust_env=False,timeout=900,limits=httpx.Limits(max_connections=256,max_keepalive_connections=128)) as client:
        async def one(i):
            nonlocal out_tokens,prompt_tokens
            async with sem:
                started=time.monotonic()
                try:
                    r=await client.post(endpoints[i%4]+'/chat/completions',json={'model':'Qwen3.8-27B','messages':[{'role':'user','content':prompt}],'max_tokens':tokens,'temperature':0,'seed':2026+i,'ignore_eos':True,'chat_template_kwargs':{'enable_thinking':False}})
                    r.raise_for_status();d=r.json();out_tokens+=d['usage']['completion_tokens'];prompt_tokens+=d['usage']['prompt_tokens'];latencies.append(time.monotonic()-started)
                except Exception as e:errors.append(str(e))
        started=time.monotonic();await asyncio.gather(*(one(i) for i in range(max(32,concurrency))));seconds=time.monotonic()-started
    return {'concurrency':concurrency,'requests':max(32,concurrency),'successful':len(latencies),'seconds':seconds,'completion_tokens':out_tokens,'prompt_tokens':prompt_tokens,'output_tokens_per_second':out_tokens/seconds,'request_latency_mean':statistics.mean(latencies) if latencies else None,'errors':errors}
p=argparse.ArgumentParser();p.add_argument('--concurrency',type=int,nargs='+',default=[8,32,100]);p.add_argument('--tokens',type=int,default=128);p.add_argument('--name',default='concurrency');p.add_argument('--input-repeat',type=int,default=1);args=p.parse_args()
results=[]
for c in args.concurrency:
    result=asyncio.run(benchmark(c,args.tokens,args.input_repeat));results.append(result);print(json.dumps(result),flush=True)
    (ROOT/'runs'/f'benchmark_{args.name}.json').write_text(json.dumps(results,indent=2)+'\n')
