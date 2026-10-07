from pathlib import Path
import json
import os
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
config=json.loads((ROOT/'configs/synthesis.json').read_text())
serving=json.loads((ROOT/'configs/serving.json').read_text()) if (ROOT/'configs/serving.json').exists() else {}
count=int(sys.argv[1]) if len(sys.argv)>1 else 4
registry=ROOT/'runs/services.json'
existing=json.loads(registry.read_text()) if registry.exists() else []
active=[]
for item in existing:
    try:os.kill(item['pid'],0);active.append(item)
    except ProcessLookupError:pass
for i in range(count):
    port=18270+i
    if any(s['port']==port for s in active):continue
    env=os.environ.copy();env.update(CUDA_VISIBLE_DEVICES=f'{2*i},{2*i+1}',TOKENIZERS_PARALLELISM='false',VLLM_USE_MODELSCOPE='0',HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',OMP_NUM_THREADS='8',VLLM_CACHE_ROOT=str(ROOT/'runs/vllm_cache'/str(i)),FLASHINFER_WORKSPACE_BASE='/tmp/privacy-qwen38-flashinfer')
    command=[str(ROOT/'.venv/bin/python'),'-m','vllm.entrypoints.openai.api_server','--model',config['teacher_model_path'],'--served-model-name',config['teacher_alias'],'--host','127.0.0.1','--port',str(port),'--tensor-parallel-size','2','--dtype','bfloat16','--max-model-len','65536','--max-num-seqs',str(serving.get('max_num_seqs',16)),'--max-num-batched-tokens','8192','--gpu-memory-utilization','0.85','--enable-auto-tool-choice','--tool-call-parser','qwen3_coder','--reasoning-parser','qwen3','--language-model-only']
    if serving.get('prefix_caching'):command.append('--enable-prefix-caching')
    if serving.get('compilation_config'):command.extend(['--compilation-config',json.dumps(serving['compilation_config'])])
    else:command.append('--enforce-eager')
    logfile=ROOT/'logs'/f'service_{port}.log'
    with logfile.open('a') as out:proc=subprocess.Popen(command,env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True,cwd=ROOT)
    active.append({'port':port,'pid':proc.pid,'gpus':[2*i,2*i+1],'serving':serving,'command':command,'log':str(logfile)})
registry.write_text(json.dumps(active,indent=2)+'\n')
print(json.dumps(active,indent=2))
