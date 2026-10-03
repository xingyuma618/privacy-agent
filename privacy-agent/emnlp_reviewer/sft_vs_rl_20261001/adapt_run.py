#!/usr/bin/env python3
"""One-time importer for legacy outputs; new evaluations must use runner.py."""
import argparse,json
from pathlib import Path
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--all-res",required=True); ap.add_argument("--execution",required=True); ap.add_argument("--cloud-trace",required=True); ap.add_argument("--output",required=True)
    a=ap.parse_args(); rows=json.loads(Path(a.all_res).read_text()); ex=json.loads(Path(a.execution).read_text()); traces=json.loads(Path(a.cloud_trace).read_text())
    out=[]
    for r in rows:
        raw_cid=r.get("masksql_idx") if r.get("masksql_idx") is not None else r.get("case_id")
        cid=str(raw_cid); status=traces.get(cid,{}).get("status","trace_missing")
        calls=traces.get(cid,{}).get("requests",[]); complete=traces.get(cid,{}).get("complete",False)
        if cid not in ex: raise ValueError(f"missing EX for {cid}")
        out.append({"question_id":cid,"case_id":cid,"execution_correct":ex[cid],"annotated_links":r.get("annotated_links"),"masked_terms":r.get("masked_terms",[]),"cost_tokens":r.get("total_tokens",0),"all_model_requests":calls,"cloud_requests":[x for x in calls if x.get("role")=="sql_generator"],"trace_complete":complete,"task_status":status})
    Path(a.output).write_text("".join(json.dumps(x,ensure_ascii=False)+"\n" for x in out))
if __name__=="__main__": main()
