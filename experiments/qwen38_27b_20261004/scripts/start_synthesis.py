import argparse,json,os,subprocess,time
from pathlib import Path
import httpx
ROOT=Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--config',type=Path,default=ROOT/'configs/synthesis_parallel100.json');p.add_argument('--run',default='full_parallel100');p.add_argument('--workers',type=int);args=p.parse_args()
config=json.loads(args.config.read_text());workers=args.workers or config['workers']
for endpoint in config['endpoints']:
    response=httpx.get(endpoint+'/models',trust_env=False,timeout=10)
    response.raise_for_status()
    assert any(m['id']==config['teacher_alias'] for m in response.json()['data']), endpoint
registry=ROOT/'runs/synthesis_process.json'
if registry.exists():
    old=json.loads(registry.read_text())
    try:os.kill(old['pid'],0)
    except ProcessLookupError:pass
    else:
        state=Path(f"/proc/{old['pid']}/stat").read_text().split(') ')[1][0]
        if state!='Z':raise SystemExit('Synthesis already running: '+str(old['pid']))
log_path=ROOT/'logs'/(args.run+'.log')
with log_path.open('ab') as log:
    command=[str(ROOT/'.venv/bin/python'),'-u',str(ROOT/'src/run_synthesis.py'),'--config',str(args.config),'--run',args.run,'--workers',str(workers)]
    proc=subprocess.Popen(command,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
record={'pid':proc.pid,'command':command,'run':args.run,'workers':workers,'started_unix':time.time(),'log':str(log_path),'config':str(args.config)}
registry.write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record,indent=2))
