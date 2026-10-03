# PrivAgent-R1 paired-eval services

All servers bind to `127.0.0.1`, use one A100 each, LMDeploy TP=1, session length 32768,
and KV-cache fraction 0.35. Checkpoints supply their own Qwen3 chat template. The SFT and
RL template files have identical SHA-256 `64f85b198065d0fba2a81f37e10ed68161ce2c19a754c7100e67e0ca2ee9c326`.

| Service | GPU | Port | Served model id |
|---|---:|---:|---|
| Query rewrite | 0 | 18336 | `query-rewrite-checkpoint-50` |
| Entity extract | 1 | 18337 | `entity-extract-checkpoint-25` |
| SFT | 2 | 18340 | `PrivAgent-R1-SFT-checkpoint-40` |
| RL | 3 | 18341 | `PrivAgent-R1-RL-step10` |

The RL artifact lives under a misleading `reward_ablation_1/checkpoint-30` directory, but
`merge_info.json` identifies it as experiment `qwen3-4b_function_rm-gsm8k-sgl-multi-w-tool-verify-n16_2`,
`global_step_10`, descended from the SFT checkpoint-40. Report it only as **RL-step10**.

On this managed execution platform, start all four with `bash supervise_services.sh` and keep
that foreground session alive. For an ordinary host, `bash start_services.sh` also works;
select services by passing names, for example `bash start_services.sh rewrite entity`. Use `bash status_services.sh` and
`bash stop_services.sh`. Logs and PID files stay below this directory.

Evaluation endpoint variables:

```bash
export QUERY_REWRITE_BASE_URL=http://127.0.0.1:18336/v1
export QUERY_REWRITE_MODEL_NAME=query-rewrite-checkpoint-50
export ENTITY_EXTRACT_BASE_URL=http://127.0.0.1:18337/v1
export ENTITY_EXTRACT_MODEL_NAME=entity-extract-checkpoint-25
```

Use SFT at `http://127.0.0.1:18340/v1` and RL-step10 at
`http://127.0.0.1:18341/v1`. For deterministic paired evaluation, requests should set
temperature 0; temperature is a request parameter, not a server startup option.
