import json,os,signal
from pathlib import Path
root=Path(__file__).resolve().parents[1]
registry=root/'runs/services.json'
for service in json.loads(registry.read_text()) if registry.exists() else []:
    try:os.killpg(service['pid'],signal.SIGTERM);print('Stopped',service['pid'])
    except ProcessLookupError:pass
