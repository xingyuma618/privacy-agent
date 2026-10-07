import argparse,json,time,re
from pathlib import Path
import httpx
ROOT=Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--seconds',type=int,default=120);p.add_argument('--interval',type=int,default=10);args=p.parse_args()
endpoints=json.loads((ROOT/'configs/synthesis_parallel100.json').read_text())['endpoints'];points=[];start=time.monotonic()
with httpx.Client(trust_env=False,timeout=10) as client:
 while time.monotonic()-start<=args.seconds:
  point={'seconds':time.monotonic()-start,'running':0,'waiting':0,'generation_tokens':0,'prompt_tokens':0,'preemptions':0,'kv_usage':[]}
  for endpoint in endpoints:
   text=client.get(endpoint[:-3]+'/metrics').text
   def get(metric):
    matches=re.findall(r'^vllm:'+metric+r'(?:\{[^\n]*\})?\s+([\d.eE+-]+)$',text,re.MULTILINE)
    return sum(float(x) for x in matches)
   point['running']+=get('num_requests_running');point['waiting']+=get('num_requests_waiting');point['generation_tokens']+=get('generation_tokens_total');point['prompt_tokens']+=get('prompt_tokens_total');point['preemptions']+=get('num_preemptions_total');point['kv_usage'].append(get('kv_cache_usage_perc'))
  if points:
   prior=points[-1];point['output_tokens_per_second']=(point['generation_tokens']-prior['generation_tokens'])/(point['seconds']-prior['seconds'])
  points.append(point);print(json.dumps(point),flush=True)
  (ROOT/'runs/throughput_real_100.json').write_text(json.dumps(points,indent=2)+'\n')
  time.sleep(args.interval)
