import json,os,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
state_path=ROOT/'runs/backend_test_state.json';state=json.loads(state_path.read_text())
command=['/tmp/privacy-qwen38-sglang-runtime/bin/python','-m','sglang.launch_server','--model-path','/mnt/wh_intern/MXY/models/Qwen3.8-27B','--served-model-name','Qwen3.8-27B','--host','127.0.0.1','--port','18280','--tp-size','2','--dtype','bfloat16','--context-length','65536','--mem-fraction-static','0.85','--max-running-requests','48','--cuda-graph-max-bs','48','--chunked-prefill-size','8192','--reasoning-parser','qwen3','--tool-call-parser','qwen3_coder','--mamba-scheduler-strategy','no_buffer','--disable-custom-all-reduce']
env=os.environ.copy();env.update(CUDA_VISIBLE_DEVICES='0,1',TOKENIZERS_PARALLELISM='false',VLLM_USE_MODELSCOPE='0',SGLANG_USE_MODELSCOPE='false',HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',OMP_NUM_THREADS='8',CUDA_HOME='/tmp/privacy-qwen38-cuda-toolkit',CUDA_PATH='/tmp/privacy-qwen38-cuda-toolkit')
with (ROOT/'logs/sglang_test.log').open('ab') as log:p=subprocess.Popen(command,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
state.update(sglang_pid=p.pid,sglang_command=command);state_path.write_text(json.dumps(state,indent=2)+'\n');print(p.pid)
