# Verified SFT-to-RL lineage for the supplementary paired evaluation

## Conclusion

The surviving merged model under `reward_ablation_1/checkpoint-30` is **not** a
reward-ablation step-30 model. Its own merge record identifies it as actor
`global_step_10` from experiment
`qwen3-4b_function_rm-gsm8k-sgl-multi-w-tool-verify-n16_2`. The retained
training log identifies its parent as the SFT model
`LlamaFactory/saves/agent_v2_deepseek_pro/checkpoint-40`.

This establishes a usable, auditable pair for a **new supplementary** SFT-vs-RL
evaluation:

1. SFT parent: `/mnt/wh_intern/MXY/paper/LlamaFactory/saves/agent_v2_deepseek_pro/checkpoint-40`
2. RL child: `/mnt/wh_intern/MXY/paper/privacy-agent/verl/output/models/qwen/Qwen3-4B-Instruct/reward_ablation_1/checkpoint-30`
3. Required label for the child: `RL global_step_10` (not `reward_ablation checkpoint-30`)

It must not be presented as a reproduction of the 65.00% EX reported in the
paper (the paper evaluation contains 300 questions).

## Evidence

- `verl/output/models/qwen/Qwen3-4B-Instruct/reward_ablation_1/checkpoint-30/merge_info.json:2-6`
  names the true experiment, source FSDP directory, `global_step_10`, actor
  component, and intended output name `checkpoint-10`.
- `verl/train.log:7` records the launch command: GRPO, parent checkpoint-40,
  actor LR `1e-6`, rollout `n=16`, train batch 64, eight GPUs, checkpoint every
  10 steps, and one requested epoch.
- `verl/train.log:450` records the resolved parent path, while lines 650 and 976
  record the resolved experiment name.
- `verl/train.log:7475-7483` records the eight-rank FSDP save at global step 10.
  The source directory still contains all eight model, optimizer, and extra-state
  shards. Its `actor/fsdp_config.json:2-3` records FSDP v1 and world size 8.
- The progress bar reaches `13/125` at `verl/train.log:7498`, followed by SIGTERM
  at lines 7499-7506. Thus this was not a one-step smoke test, but it was also not
  a completed one-epoch run. The defensible artifact is the intermediate step-10
  checkpoint.
- Despite the legacy project directory name `gsm8k_async_rl`, the resolved run
  is demonstrably the privacy NL2SQL task: `verl/train.log:584` selects rollout
  format `privacy_agent`; lines 591 and 845 select `privacy_tools.yaml`; lines
  847 and 853 select `data/privacy-agent/{train,test}.parquet`; and line 931
  selects `verl/utils/reward_score/nl2sql.py`.
- The parent SFT `trainer_state.json:5-7,38,42,44` records global step 40,
  epoch 1.1779, max steps 68, and two requested epochs.

The source FSDP shards were written around 18:44 on 2026-05-24 and the merged
weights around 21:41 the same day. This ordering is consistent with the merge
record. The merged model has two safetensor shards and a complete index and
tokenizer set. The merged and SFT `config.json` files have the same SHA-256,
which is expected because architecture config does not encode learned weight
changes; this hash equality is not itself evidence that the weights are equal.

## Reward configuration caveat

The retained run config points to `verl/utils/reward_score/nl2sql.py`, and the
historical log visibly reports four components: execution, privacy, normalized
length, and turn rewards (for example, `verl/train.log:1824`). The retained
launch/config does not contain explicit lambda overrides, and the historical log
does not print independently recoverable lambda values. Current source defaults
(`nl2sql.py:420-421,440-441`) are 2.0/2.0, but current code must not be used as
proof of the exact historical weights without a commit-pinned source snapshot.

## Remaining uncertainty

- No surviving artifact proves that this checkpoint corresponds to the deleted
  historical `reward_ablation/checkpoint-{10,20,30}` evaluations.
- The run ended before its planned 125 steps; no claim of converged or final RL
  performance is warranted.
- The misleading merged directory name should be treated only as a filesystem
  location. Reports and tables should use the verified experiment and step.
- `deepseek-chat` is a provider alias, not a durable model identity. The retained
  historical evidence does not contain the provider-returned SQL-generator model
  ID. Any new supplementary evaluation must record the response's provider model
  ID (but never credentials) alongside the configured alias.
- Earlier sibling experiments (without suffix and with suffix `_1`) retain FSDP
  step-20 directories, but no retained log proves their parent/config chain as
  directly as this `_2` run. They also predate `_2` (May 21-22 versus May 24),
  so there is no later same-series checkpoint with an equally verified lineage.
