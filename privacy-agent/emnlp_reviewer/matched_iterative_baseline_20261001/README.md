# Fixed iterative baseline

This experiment answers the reviewer question about whether a scripted workflow
with the same trained tools and an execution-triggered retry can match the
adaptive agent.

Both fixed variants always run query rewrite, entity extraction, typed masking,
SQL generation, local unmasking, and a read-only SQLite execution check. The
iterative variant performs one additional SQL generation/unmask/check pass only
when the first SQL fails to execute. Before the retry, raw names and values in
the SQLite error are converted back to their placeholders. The one-pass variant
uses the same code and stops after its first check.

The rewrite, entity extraction, and SQL generator checkpoints are identical to
the local adaptive-agent evaluation in `../sft_vs_rl_20261001`. Two replicas of
each service use GPUs 0-7. The summary compares both fixed variants with the
three saved SFT and RL adaptive-agent repetitions on the same 300 canonical
MaskSQL/BIRD questions.

Run:

```bash
bash services/start_services.sh
bash run_all.sh
```

Outputs are written to `results/` and `summary.json`.

For the one-run generalization check on full BIRD dev, Spider dev, and Spider
test, run `bash run_generalization.sh`.  Those outputs are kept separately in
`results_generalization/` and `generalization_summary.json`; this avoids
presenting a single run as if it had the same three-seed uncertainty estimate
as the primary 300-question comparison.

`run_adaptive_generalization.sh` additionally runs the SFT adaptive controller
on the first 300 canonical cases of Spider dev and Spider test.  Its summary
compares those exact IDs with the corresponding rows from the full fixed
workflow runs, under the same local SQL generator and tool checkpoints.  The
completed outputs are in `results_adaptive_generalization_4rep/`; the similarly
named directory without `_4rep` is an explicitly marked interrupted throughput
trial and is not read by the summary.
