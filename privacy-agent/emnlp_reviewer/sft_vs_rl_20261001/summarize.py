#!/usr/bin/env python3
"""Strict paired summary over model/run-{1,2,3}/cases.jsonl (stdlib only)."""
import argparse, hashlib, json, math, random, statistics
from pathlib import Path

ROLES=("agent","query_rewrite","entity_extract","sql_generator")
REPLICA_ENDPOINTS={"sft":"http://127.0.0.1:18346/v1","rl":"http://127.0.0.1:18347/v1"}
METRICS=("EX","legacy_MR","bounded_MP","bounded_MR","bounded_MF1","cost_tokens")+tuple(f"{kind}_{role}" for role in ROLES for kind in ("calls","prompt_tokens","completion_tokens","total_tokens"))

def norm(x): return " ".join(x.strip(" '\"`").lower().split())
def edge(g, p):
    g,p=norm(g),norm(p); return bool(g and p and (g == p or g in p or p in g))
def maximum_matching(gold, pred):
    adj=[[j for j,p in enumerate(pred) if edge(g,p)] for g in gold]; match={}
    def visit(i, seen):
        for j in adj[i]:
            if j in seen: continue
            seen.add(j)
            if j not in match or visit(match[j], seen): match[j]=i; return True
        return False
    return sum(visit(i,set()) for i in range(len(gold)))
def unique_terms(values):
    out=[]; seen=set()
    for value in values:
        key=norm(value)
        if key and key not in seen: seen.add(key); out.append(value)
    return out
def case_metrics(row):
    if "annotated_links" in row:
        if not isinstance(row["annotated_links"],dict): raise ValueError("annotated_links must be an object")
        legacy_gold=list(row["annotated_links"])
    else:
        if not isinstance(row.get("annotated_terms"),list): raise ValueError("annotated_terms must be a list")
        legacy_gold=row["annotated_terms"]
    pred_raw=row.get("masked_terms")
    if any(not isinstance(x,str) or not x.strip() for x in legacy_gold): raise ValueError("gold terms must be nonempty strings")
    if not isinstance(pred_raw,list) or any(not isinstance(x,str) or not x.strip() for x in pred_raw): raise ValueError("masked_terms must be nonempty strings")
    # Preserve historical MR's original gold keys; bounded metrics independently deduplicate.
    covered=sum(any(edge(g,p) for p in pred_raw) for g in legacy_gold)
    gold=unique_terms(legacy_gold); pred=unique_terms(pred_raw)
    matched=maximum_matching(gold,pred)
    mp=matched/len(pred) if pred else (1.0 if not gold else 0.0)
    mr=matched/len(gold) if gold else 1.0
    metrics={"EX": float(row["execution_correct"]), "legacy_MR": covered/len(legacy_gold) if legacy_gold else 1.0,
            "bounded_MP":mp, "bounded_MR":mr, "bounded_MF1":2*mp*mr/(mp+mr) if mp+mr else 0.0,
            "cost_tokens":float(row["cost_tokens"])}
    requests=row.get("all_model_requests",[])
    for role in ROLES:
        calls=[x for x in requests if x.get("role")==role]; metrics[f"calls_{role}"]=float(len(calls))
        for kind in ("prompt_tokens","completion_tokens","total_tokens"):
            metrics[f"{kind}_{role}"]=float(sum((x.get("usage") or {}).get(kind) or 0 for x in calls))
    return metrics
def validate_requests(value,cid):
    if not isinstance(value,list): raise ValueError(f"all_model_requests must be a list for {cid}")
    required={"role","endpoint","request_model","response_model","ok","usage","messages"}
    for i,x in enumerate(value):
        if not isinstance(x,dict) or required-x.keys(): raise ValueError(f"invalid model request {i} for {cid}")
        if not isinstance(x["role"],str) or not isinstance(x["endpoint"],str) or not isinstance(x["request_model"],str) or not isinstance(x["ok"],bool) or not isinstance(x["usage"],dict) or not isinstance(x["messages"],list): raise ValueError(f"invalid model request {i} for {cid}")
        for key in ("prompt_tokens","completion_tokens","total_tokens"):
            v=x["usage"].get(key,0)
            if v is not None and (isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) or v<0): raise ValueError(f"invalid {key} in model request {i} for {cid}")

def validate_routing(rows, meta, manifest, model, run, wrapper_sha256):
    model_spec=manifest["models"][model]; alias=model_spec["served_model_name"]; primary=model_spec["base_url"]
    tools=manifest["inference"]["tools_checkpoint"]; generator=manifest["inference"]["cloud_generator"]
    expected={
        "query_rewrite":(tools["query_rewrite_base_url"],tools["query_rewrite_model"]),
        "entity_extract":(tools["entity_extract_base_url"],tools["entity_extract_model"]),
        "sql_generator":(generator["base_url"],generator["model_name"]),
    }
    if run==2:
        routing=meta.get("agent_routing")
        required={"logical_endpoint","actual_endpoint","checkpoint_path","wrapper_sha256","verified_served_model","serving_origin"}
        if not isinstance(routing,dict) or set(routing)!=required or any(not isinstance(v,str) for v in routing.values()): raise ValueError(f"{model}/run-2: invalid agent_routing provenance")
        wanted={"logical_endpoint":primary,"actual_endpoint":REPLICA_ENDPOINTS[model],"checkpoint_path":model_spec["path"],"wrapper_sha256":wrapper_sha256,"verified_served_model":alias,"serving_origin":"approved_local_replica_same_checkpoint"}
        if routing!=wanted: raise ValueError(f"{model}/run-2: agent_routing provenance mismatch")
        agent_endpoint=routing["actual_endpoint"]
    else:
        if "agent_routing" in meta: raise ValueError(f"{model}/run-{run}: direct-primary run must not declare replica routing")
        agent_endpoint=primary
    def request_matches(request, endpoint, expected_alias):
        if request.get("endpoint")!=endpoint or request.get("request_model")!=expected_alias: return False
        response_model=request.get("response_model")
        return response_model==expected_alias if request.get("ok") is True else response_model is None
    for row in rows:
        cid=row.get("case_id"); requests=row.get("all_model_requests",[])
        if any(x.get("role") not in ROLES for x in requests): raise ValueError(f"unknown or hidden model request role for {cid}")
        agent_calls=[x for x in requests if x.get("role")=="agent"]
        if row.get("task_status")=="valid" and not agent_calls: raise ValueError(f"valid case has no agent request for {cid}")
        for request in agent_calls:
            if not request_matches(request,agent_endpoint,alias): raise ValueError(f"invalid agent routing/model identity for {cid}")
        for role,(endpoint,role_alias) in expected.items():
            for request in requests:
                if request.get("role")==role and not request_matches(request,endpoint,role_alias): raise ValueError(f"invalid {role} routing/model identity for {cid}")

def validate_recovery(rows, meta, expected_ids, model, run, driver_path, approved_source):
    origin=meta.get("inference_origin")
    if origin is None: return None
    if origin!="segmented_recovery" or model!="sft" or run!=1: raise ValueError("segmented recovery is allowed only for sft/run-1")
    provenance=meta.get("recovery_provenance"); keys={"source_directory","source_cases_sha256","source_metadata_sha256","retained_question_ids","missing_question_ids","driver_sha256","source_model","source_run"}
    if not isinstance(provenance,dict) or set(provenance)!=keys: raise ValueError("invalid recovery provenance schema")
    source=Path(provenance.get("source_directory",""))
    if not source.is_absolute() or source.resolve()!=Path(approved_source).resolve(): raise ValueError("recovery source is not the approved same-repetition archive")
    cases_path=source/"cases.jsonl"; metadata_path=source/"metadata.json"
    if not cases_path.is_file() or not metadata_path.is_file() or not Path(driver_path).is_file(): raise ValueError("recovery provenance file is missing")
    sha=lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
    if provenance.get("source_cases_sha256")!=sha(cases_path) or provenance.get("source_metadata_sha256")!=sha(metadata_path) or provenance.get("driver_sha256")!=sha(driver_path): raise ValueError("recovery provenance hash mismatch")
    if provenance.get("source_model")!="sft" or provenance.get("source_run")!=1: raise ValueError("cross-repetition recovery source rejected")
    source_meta=json.loads(metadata_path.read_text()); source_rows=[json.loads(x) for x in cases_path.read_text().splitlines() if x.strip()]
    retained=provenance.get("retained_question_ids"); missing=provenance.get("missing_question_ids")
    if not isinstance(retained,list) or not isinstance(missing,list) or any(not isinstance(x,str) for x in retained+missing): raise ValueError("invalid recovery ID lists")
    source_ids=[r.get("case_id") for r in source_rows]
    canonical_retained=[x for x in expected_ids if x!="bird_122"]
    if retained!=canonical_retained or missing!=["bird_122"] or set(source_ids)!=set(retained) or len(source_ids)!=len(set(source_ids)) or len(source_ids)!=299: raise ValueError("recovery source IDs are not the approved 299+1 partition")
    if set(retained)&set(missing) or set(retained)|set(missing)!=set(expected_ids): raise ValueError("recovery IDs are not disjoint and complete")
    frozen=("model","run","config_hash","manifest_sha256","model_identity","fresh_output_dir","workers","limit","runner_sha256","legacy_evaluator_sha256","prompt_template_sha256","question_ids")
    if any(source_meta.get(k)!=meta.get(k) for k in frozen) or source_meta.get("model")!="sft" or source_meta.get("run")!=1: raise ValueError("recovery source frozen metadata mismatch")
    if source_meta.get("status")!="running" or source_meta.get("completed_cases")!=299 or meta.get("started_unix")!=source_meta.get("started_unix"): raise ValueError("recovery source lifecycle metadata mismatch")
    if not isinstance(meta.get("recovery_started_unix"),(int,float)) or isinstance(meta.get("recovery_started_unix"),bool): raise ValueError("invalid recovery start time")
    final_by_id={r.get("case_id"):r for r in rows}; source_by_id={r["case_id"]:r for r in source_rows}
    if any(final_by_id.get(cid)!=source_by_id[cid] for cid in retained): raise ValueError("retained recovery row differs from source")
    if missing[0] not in final_by_id or missing[0] in source_by_id: raise ValueError("recovered missing row is absent or reused")
    return {"model":"sft","run":1,"inference_origin":origin,"source_directory":str(source),"retained_cases":299,"newly_inferred_cases":missing,"source_cases_sha256":provenance["source_cases_sha256"],"source_metadata_sha256":provenance["source_metadata_sha256"],"driver_sha256":provenance["driver_sha256"]}

def load_run(path, expected_ids, sql_generator_endpoint):
    rows=[json.loads(x) for x in path.read_text().splitlines() if x.strip()]; out={}
    required={"case_id","question_id","execution_correct","masked_terms","cost_tokens","cloud_requests","all_model_requests","trace_complete","task_status"}
    for r in rows:
        missing=required-r.keys()
        if missing: raise ValueError(f"{path}: missing {sorted(missing)}")
        cid=r["case_id"]
        if not isinstance(cid,str) or cid != r["question_id"]: raise ValueError(f"{path}: invalid canonical ID {cid!r}")
        if cid in out: raise ValueError(f"{path}: duplicate case_id {cid}")
        if "annotated_links" not in r and "annotated_terms" not in r: raise ValueError(f"{path}: no gold annotation field for {cid}")
        if type(r["execution_correct"]) not in (bool,int) or r["execution_correct"] not in (False,True,0,1): raise ValueError(f"{path}: invalid EX for {cid}")
        if isinstance(r["cost_tokens"],bool) or not isinstance(r["cost_tokens"],(int,float)) or not math.isfinite(r["cost_tokens"]) or r["cost_tokens"] < 0: raise ValueError(f"{path}: invalid cost for {cid}")
        if not isinstance(r["cloud_requests"],list) or not isinstance(r["trace_complete"],bool): raise ValueError(f"{path}: invalid trace for {cid}")
        validate_requests(r["all_model_requests"],cid)
        if any(x not in r["all_model_requests"] for x in r["cloud_requests"]): raise ValueError(f"{path}: cloud trace not contained in full trace for {cid}")
        if any(not isinstance(x,dict) or x.get("role")!="sql_generator" for x in r["cloud_requests"]): raise ValueError(f"{path}: invalid cloud request role for {cid}")
        sql_calls=[x for x in r["all_model_requests"] if x.get("role")=="sql_generator"]
        if any(x.get("endpoint")!=sql_generator_endpoint for x in sql_calls): raise ValueError(f"{path}: sql generator endpoint is not the locked local endpoint for {cid}")
        if r["cloud_requests"]!=sql_calls: raise ValueError(f"{path}: cloud_requests must equal the complete sql_generator trace for {cid}")
        if r["task_status"] not in {"valid","infrastructure_error"}: raise ValueError(f"{path}: invalid task status for {cid}")
        if r["task_status"] == "valid" and r["trace_complete"] is not True: raise ValueError(f"{path}: valid case has incomplete trace for {cid}")
        if r["task_status"] != "valid" and r["execution_correct"] not in (False,0): raise ValueError(f"{path}: failed case must have EX=0: {cid}")
        out[cid]=case_metrics(r)
    if set(out)!=set(expected_ids): raise ValueError(f"{path}: canonical IDs mismatch")
    return out
def avg(xs): return sum(xs)/len(xs)
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("results_dir"); ap.add_argument("--manifest",default=str(Path(__file__).resolve().parent/"manifest.json")); ap.add_argument("--preflight",default=str(Path(__file__).resolve().parent/"preflight_report.json"))
    ap.add_argument("--bootstrap",type=int,default=10000); ap.add_argument("--seed",type=int,default=20261001); ap.add_argument("--output")
    a=ap.parse_args(); root=Path(a.results_dir); data={}; manifest_path=Path(a.manifest); manifest=json.loads(manifest_path.read_text()); locked_hash=hashlib.sha256(manifest_path.read_bytes()).hexdigest(); pf=json.loads(Path(a.preflight).read_text())
    wrapper_path=Path(__file__).resolve().parent/"run_replica_eval.py"
    if not wrapper_path.is_file(): raise ValueError("replica routing wrapper is missing")
    wrapper_sha256=hashlib.sha256(wrapper_path.read_bytes()).hexdigest()
    recovery_driver=Path(__file__).resolve().parent/"run_missing_recovery.py"
    approved_recovery_source=Path(__file__).resolve().parent/"results_local_qwen_partial_archive"/"sft-run-1-interrupted-299-20261001"
    if pf.get("status")!="ready" or pf.get("artifacts",{}).get("manifest_sha256")!=locked_hash: raise ValueError("preflight is not ready for current manifest")
    annotations=json.loads(Path(manifest["dataset"]["masksql_annotations"]).read_text()); expected_ids=[x["idx"] for x in annotations]
    sql_generator_endpoint=manifest["inference"]["cloud_generator"]["base_url"]
    if sql_generator_endpoint!="http://127.0.0.1:18342/v1": raise ValueError("manifest does not lock sql_generator to local 127.0.0.1:18342")
    if len(expected_ids)!=manifest["dataset"]["expected_cases"] or len(set(expected_ids))!=len(expected_ids): raise ValueError("invalid canonical annotation ID set")
    metadata={}; config_hashes=set(); runner_hashes=set(); legacy_hashes=set(); prompt_hashes=set(); infra=[]; recovery_notes=[]
    for model in ("sft","rl"):
        data[model]=[]; metadata[model]=[]
        for i in range(1,4):
            d=root/model/f"run-{i}"; meta=json.loads((d/"metadata.json").read_text())
            for key in ("status","completed_cases","config_hash","manifest_sha256","model_identity","fresh_output_dir","model","run","question_ids","workers","limit","runner_sha256","legacy_evaluator_sha256","prompt_template_sha256"):
                if key not in meta: raise ValueError(f"{d}/metadata.json missing {key}")
            if meta["fresh_output_dir"] is not True: raise ValueError(f"{d}: output was not fresh")
            if meta["manifest_sha256"]!=locked_hash: raise ValueError(f"{d}: manifest hash mismatch")
            if meta["model_identity"]!=pf["models"][model]: raise ValueError(f"{d}: model identity mismatch")
            if meta["model"]!=model or meta["run"]!=i: raise ValueError(f"{d}: model/run metadata mismatch")
            if meta["status"]!="complete" or meta["completed_cases"]!=len(expected_ids): raise ValueError(f"{d}: run metadata is not complete")
            if meta["limit"]!=0: raise ValueError(f"{d}: formal run must use limit=0")
            if not isinstance(meta["workers"],int) or isinstance(meta["workers"],bool) or meta["workers"]<1: raise ValueError(f"{d}: invalid workers")
            if len(meta["question_ids"])!=len(expected_ids) or set(meta["question_ids"])!=set(expected_ids): raise ValueError(f"{d}: metadata IDs mismatch")
            if not isinstance(meta["prompt_template_sha256"],dict) or set(meta["prompt_template_sha256"])!={"query_rewrite_template.py","entity_extract_template.py","sql_generate_template.py"}: raise ValueError(f"{d}: invalid prompt template hashes")
            config_hashes.add(meta["config_hash"]); runner_hashes.add(meta["runner_sha256"]); legacy_hashes.add(meta["legacy_evaluator_sha256"]); prompt_hashes.add(json.dumps(meta["prompt_template_sha256"],sort_keys=True)); metadata[model].append(meta)
            rows=[json.loads(x) for x in (d/"cases.jsonl").read_text().splitlines() if x.strip()]
            note=validate_recovery(rows,meta,expected_ids,model,i,recovery_driver,approved_recovery_source)
            if note: recovery_notes.append(note)
            validate_routing(rows,meta,manifest,model,i,wrapper_sha256)
            infra += [(model,i,str(r.get("case_id")),r.get("task_status")) for r in rows if r.get("task_status") != "valid"]
            data[model].append(load_run(d/"cases.jsonl",expected_ids,sql_generator_endpoint))
    if any(len(x)!=1 for x in (config_hashes,runner_hashes,legacy_hashes,prompt_hashes)): raise ValueError("runs do not share locked config/source/prompt metadata")
    worker_schedule=[]
    for i in range(3):
        sft_workers=metadata["sft"][i]["workers"]; rl_workers=metadata["rl"][i]["workers"]
        if sft_workers!=rl_workers: raise ValueError(f"run-{i+1}: paired SFT/RL workers mismatch")
        worker_schedule.append(sft_workers)
    ids=set(data["sft"][0])
    for model,runs in data.items():
        for i,r in enumerate(runs,1):
            if set(r)!=ids: raise ValueError(f"unpaired IDs in {model}/run-{i}")
    per_run={m:[{k:avg([r[c][k] for c in ids]) for k in METRICS} for r in rs] for m,rs in data.items()}
    overall={m:{k:{"mean":avg([x[k] for x in rr]),"sample_sd":statistics.stdev([x[k] for x in rr])} for k in METRICS} for m,rr in per_run.items()}
    # Cluster unit is case: average repetitions within each case, then resample cases.
    delta_by_case={k:[avg([data["rl"][j][c][k] for j in range(3)])-avg([data["sft"][j][c][k] for j in range(3)]) for c in sorted(ids)] for k in METRICS}
    rng=random.Random(a.seed); bootstrap={}
    for k,vals in delta_by_case.items():
        draws=sorted(avg([vals[rng.randrange(len(vals))] for _ in vals]) for _ in range(a.bootstrap))
        lo=draws[math.floor(.025*(len(draws)-1))]; hi=draws[math.ceil(.975*(len(draws)-1))]
        bootstrap[k]={"rl_minus_sft":avg(vals),"paired_cluster_bootstrap_95ci":[lo,hi]}
    report={"status":"blocked_infrastructure_errors" if infra else "complete","formal_conclusion_allowed":not bool(infra),"infrastructure_errors":infra,"n_cases":len(ids),"runs":3,"worker_schedule":worker_schedule,"recovery_notes":recovery_notes,"definitions":{"deduplication":"legacy gold keys preserved; bounded gold and predictions independently normalized and deduplicated","legacy_MR":"original gold keys covered by any bidirectional-substring prediction / original gold keys","bounded":"maximum one-to-one bipartite matching using the same surface edge; surface matching is not semantic privacy correctness","bootstrap":"average runs per case, then resample cases","cost_accounting":"formal token means include exactly the 300 saved complete case traces per repetition; interrupted or redundant attempts retained only in recovery logs are excluded"},"per_run":per_run,"overall_across_run_means":overall,"paired":bootstrap}
    text=json.dumps(report,indent=2)+"\n"; print(text,end="")
    if a.output: Path(a.output).write_text(text)
if __name__=="__main__": main()
