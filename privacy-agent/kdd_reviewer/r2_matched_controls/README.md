# Reviewer 2: matched controls

This directory contains the reproducible setup for the two controls requested
by Reviewer 2.

## 1. Fixed pipeline with a matched retry budget

The completed implementation and raw outputs are in:

`../../emnlp_reviewer/matched_iterative_baseline_20261001/`

The fixed controller always runs rewrite, extraction, typed masking, SQL
generation, local unmasking, and read-only execution verification.  It permits
one extra SQL-generation attempt only after the first execution failure.  The
fixed and adaptive methods use the same rewrite, extraction, and SQL-generator
checkpoints.  The primary 300-question experiment contains three fresh runs.

## 2. MCTS versus Prompt Synthesis with 3,101 trajectories each

Create equal-size datasets:

```bash
python kdd_reviewer/r2_matched_controls/prepare_matched_cold_start.py \
  --prompt LlamaFactory/data/agent/deepseek_pro/bird_train_sc_filtered_deepseek_pro_with_tools.jsonl \
  --mcts LlamaFactory/data/agent/mcts/bird_train_sc_filtered_mcts_with_tools.jsonl \
  --output-dir kdd_reviewer/r2_matched_controls/generated
```

Add these entries to `LlamaFactory/data/dataset_info.json` for the run:

```json
"r2_prompt_synthesis_n3101": {
  "file_name": "/mnt/wh_intern/MXY/paper/privacy-agent/kdd_reviewer/r2_matched_controls/generated/prompt_synthesis_n3101.jsonl",
  "formatting": "sharegpt",
  "columns": {"messages": "messages", "tools": "tools"},
  "tags": {"role_tag": "role", "content_tag": "content", "user_tag": "user", "assistant_tag": "assistant", "system_tag": "system", "observation_tag": "tool"}
},
"r2_mcts_synthesis_n3101": {
  "file_name": "/mnt/wh_intern/MXY/paper/privacy-agent/kdd_reviewer/r2_matched_controls/generated/mcts_synthesis_n3101.jsonl",
  "formatting": "sharegpt",
  "columns": {"messages": "messages", "tools": "tools"},
  "tags": {"role_tag": "role", "content_tag": "content", "user_tag": "user", "assistant_tag": "assistant", "system_tag": "system", "observation_tag": "tool"}
}
```

Train both models with the YAML files in this directory using 8-card DDP.  All settings,
including base model, number of examples, epochs, optimizer schedule, seed, and
data seed, are identical. Both runs use DeepSpeed ZeRO-2, an 8,192-token cutoff, and gradient
checkpointing; longer trajectories are truncated identically. Evaluate both checkpoints on the same 300 question
IDs with the same three inference seeds and report EX, M-P, M-R, and M-F1.

## 3. Matched synthesis-generation budget

The historical generation scripts did not save API token usage.  They therefore
cannot support a retrospective equal-token claim.  The budget-controlled rerun
must log every synthesis-model request and stop each method at the same total
completion-token budget over the same 8,451 inputs.  Tool-model calls must also
be reported separately.  After generation, retain successful trajectories with
the same privacy and execution filters, then draw the same number of retained
trajectories from each method before SFT.

Report both controls because they answer different questions:

- equal synthesis-token budget measures search efficiency;
- equal retained sample count measures trajectory quality during SFT.

Do not describe the old 4,313-versus-3,101 comparison as budget matched.

## 4. Trained checkpoints

Both equal-sample models use Qwen3-4B-Instruct-2507, 3,101 trajectories,
two epochs, seed/data seed 2026, DeepSpeed ZeRO-2, gradient checkpointing, and
an 8,192-token cutoff:

- Prompt Synthesis: `../../LlamaFactory/saves/r2_prompt_synthesis_n3101`
  (train loss 0.3155; runtime 47m05s)
- MCTS: `../../LlamaFactory/saves/r2_mcts_synthesis_n3101`
  (train loss 0.3259; runtime 55m49s)

The auxiliary inference services are started and stopped with
`start_aux_services.sh` and `stop_aux_services.sh`. Evaluation uses the existing
multi-checkpoint evaluator, the same 300 MaskSQL/BIRD cases, and the same local
rewrite, extraction, and Qwen3.8-27B SQL-generation checkpoints for both arms.
Run the complete paired evaluation with `run_eval_300.sh`. It assigns one
controller checkpoint to each of GPUs 0 and 1 and uses eight workers per arm;
this changes throughput only, not the per-case 20-turn limit or tool sequence.
`run_eval_300_sharded.sh` is the faster equivalent used for the final run: it
splits the fixed ordered set into non-overlapping rows 0--149 and 150--299,
runs one replica per method on each shard, and keeps the same eight workers and
per-case limits. The original BIRD row IDs are retained in every output.
`wait_and_merge.sh` waits for both shard summaries and then runs the merge and
300-unique-ID checks automatically.
