# SFT-before-RL vs RL-after paired evaluation

This is a new supplementary comparison, not a reproduction of the paper's
reported 65% EX. The SFT parent is `agent_v2_deepseek_pro/checkpoint-40`; the
RL child is the merged `global_step_10` artifact stored in the historically
misnamed `reward_ablation_1/checkpoint-30` directory. See `LINEAGE.md`.

The locked design uses the same 300 MaskSQL/BIRD cases for both arms, three
fresh repetitions, temperature 0, 20 agent turns, and token limits 2048
(agent), 512 (rewrite), 512 (entity), and 1024 (SQL generator). All served
aliases and ports are fixed in `manifest.json`. SQL generation uses the same
local `/mnt/wh_intern/MXY/models/Qwen3.8-27B` checkpoint for both arms; every
response's actual `response_model` is recorded. This local-backend experiment
does not reproduce or estimate the paper's cloud-backed absolute score.

## Before a run

No cloud credential is read or required. The runner rejects a generator URL
unless it is an explicit loopback address and supplies only the local server's
dummy `EMPTY` key. Start all five local services, then run `python3 preflight.py`.

Verify `/v1/models` exposes exactly the aliases in the manifest, then use fresh
smoke directories:

```bash
python3 runner.py sft 1 --output smoke/sft-5 --limit 5 --workers 1 --execute-reviewed
python3 runner.py rl 1 --output smoke/rl-5 --limit 5 --workers 1 --execute-reviewed
```

Only after reviewing both smoke runs should the six formal runs be launched.
Formal runs use `--limit 0`, the same worker count, and distinct empty paths:

```bash
python3 runner.py sft 1 --output results_local_qwen/sft/run-1 --workers 8 --execute-reviewed
python3 runner.py rl 1 --output results_local_qwen/rl/run-1 --workers 8 --execute-reviewed
```

Repeat for runs 2 and 3. Ordinarily, a partial directory is not resumed in
place: the formal output remains a new empty directory. A narrowly scoped
same-repetition recovery may retain already completed cases only when the
recovery driver records and verifies the immutable source archive, source and
driver hashes, the exact retained/missing ID partition, frozen run metadata,
and byte-decoded JSON object equality for every retained row. Cross-repetition
reuse remains forbidden. Each newly completed question is appended and fsynced
immediately.

### Recovery throughput schedule

The user authorized higher inference concurrency for recovery after the
interrupted first SFT repetition. Run 1 remains at 8 workers in both arms so
that it matches the already completed RL run. Runs 2 and 3 use 16 workers in
both arms, giving the paired worker schedule `[8, 16, 16]`. This is only a
throughput-scheduling change: per-question temperature, turn limit, token
budgets, model checkpoints, prompts, tools, and SQL timeouts remain frozen.
The summary validator requires the SFT and RL worker counts to match within
each repetition and records the three values as `worker_schedule`.

Run 2 may route the agent role through approved data-parallel serving replicas:
the SFT checkpoint on GPU 6 at port 18346 and the RL checkpoint on GPU 7 at
port 18347. These replicas load the exact same checkpoint paths and served-model
aliases as their primary endpoints; they are not additional trained models.
The replica wrapper records the logical primary endpoint, actual local endpoint,
checkpoint path, served alias, wrapper hash, and verified serving origin, while
the summary validator checks that provenance and every traced request. Runs 1
and 3 use the primary endpoints directly. Each repetition is a separate full
inference pass over all 300 canonical cases; no case outputs are reused between
repetitions. Replica routing changes only service scheduling, not per-question
temperature, turn or token budgets, prompts, tools, checkpoints, or SQL limits.

The interrupted SFT run 1 uses this audited same-repetition mechanism: 299
complete case objects are retained unchanged from its archive and only the
single missing canonical case is inferred into a fresh formal directory. The
summary exposes this in `recovery_notes`; it does not describe the assembly as
a separate independent repetition. Formal token means count exactly the 300
saved complete traces. Interrupted or redundant attempts remain available in
the recovery logs but are not added to formal token means.

`all_model_requests` contains deep-copied requests for the agent, rewrite,
entity, and SQL-generator models. `cloud_requests` is retained as a schema-
compatibility name for the logical SQL-generator subset; in this experiment
those requests go to localhost and never leave the machine. `trace_complete`
means tracing covered the full attempted case; it does
not mean every request succeeded. HTTP/authentication/connection failures are
infrastructure errors, while predicted-SQL errors, the 20-turn limit, and
context/budget exhaustion are valid task failures with EX=0. Gold SQL or DB
failures are infrastructure errors. Both agent checks and final EX use
read-only SQLite; EX follows BIRD's `set(rows)` equality.

After all six formal directories are complete:

```bash
python3 summarize.py results_local_qwen --output results_local_qwen/summary.json
python3 -m unittest -v test_summarize.py
```

Pairing and bootstrap resampling are by canonical annotation `idx`
(`question_id`), with repetitions first averaged within each question.
