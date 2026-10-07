"""Suspend the running synthesis in RAM and restore its original TP=2 vLLM service."""
import json,os,signal,subprocess,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];state_path=ROOT/'runs/backend_test_state.json'
command=sys.argv[1]
if command=='pause':
 reg=json.loads((ROOT/'runs/synthesis_process.json').read_text());pid=reg['pid']
 argv=Path(f'/proc/{pid}/cmdline').read_bytes()
 assert str(ROOT/'src/run_synthesis.py').encode() in argv
 services=json.loads((ROOT/'runs/services.json').read_text());service=next(s for s in services if s['port']==18270)
 state={'synthesis':reg,'original_service':service,'paused_unix':time.time(),'restored':False}
 state_path.write_text(json.dumps(state,indent=2)+'\n');os.kill(pid,signal.SIGSTOP);print('Suspended synthesis in RAM:',pid)
elif command=='restore':
 state=json.loads(state_path.read_text())
 if state.get('restored'):raise SystemExit('Already restored')
 # Only stop the SGLang process explicitly recorded by this test.
 if state.get('sglang_pid'):
  try:os.killpg(state['sglang_pid'],signal.SIGTERM)
  except ProcessLookupError:pass
  for _ in range(45):
   p=Path(f"/proc/{state['sglang_pid']}/stat")
   if not p.exists() or p.read_text().split(') ')[1][0]=='Z':break
   time.sleep(1)
 services=json.loads((ROOT/'runs/services.json').read_text());original=state['original_service']
 present=next((s for s in services if s['port']==18270),None)
 alive=False
 if present:
  p=Path(f"/proc/{present['pid']}/stat")
  alive=p.exists() and p.read_text().split(') ')[1][0]!='Z'
 if not alive:
  env=os.environ.copy();env.update(CUDA_VISIBLE_DEVICES='0,1',TOKENIZERS_PARALLELISM='false',VLLM_USE_MODELSCOPE='0',HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',OMP_NUM_THREADS='8',VLLM_CACHE_ROOT=str(ROOT/'runs/vllm_cache/0'),FLASHINFER_WORKSPACE_BASE='/tmp/privacy-qwen38-flashinfer')
  with (ROOT/'logs/service_18270.log').open('ab') as log:p=subprocess.Popen(original['command'],env=env,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
  restored=dict(original,pid=p.pid)
  services=[s for s in services if s['port']!=18270]+[restored];(ROOT/'runs/services.json').write_text(json.dumps(services,indent=2)+'\n')
 import httpx
 deadline=time.monotonic()+300
 while time.monotonic()<deadline:
  try:
   r=httpx.get('http://127.0.0.1:18270/v1/models',trust_env=False,timeout=3)
   if r.status_code==200:break
  except Exception:pass
  time.sleep(2)
 else:raise SystemExit('Original service still not ready; synthesis remains suspended')
 os.kill(state['synthesis']['pid'],signal.SIGCONT);state.update(restored=True,resumed_unix=time.time());state_path.write_text(json.dumps(state,indent=2)+'\n');print('Restored vLLM and resumed synthesis',state['synthesis']['pid'])
else:raise SystemExit('Use pause or restore')
