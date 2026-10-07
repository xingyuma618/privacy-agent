import json,os,signal
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
p=ROOT/'runs/synthesis_process.json'
if p.exists():
    d=json.loads(p.read_text())
    try:os.killpg(d['pid'],signal.SIGTERM);print('Stopped',d['pid'])
    except ProcessLookupError:print('Already stopped')
