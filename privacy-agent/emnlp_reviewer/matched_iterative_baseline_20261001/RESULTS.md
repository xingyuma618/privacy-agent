# Fixed iterative baseline: results

## What was compared

The fixed controller always applies query rewriting, entity extraction, typed
masking, SQL generation, local unmasking, and a read-only execution check.  If
the SQL fails to execute, the iterative version makes one more SQL-generation
attempt.  Database errors are converted back to placeholders before they are
sent to the SQL generator.  The adaptive and fixed methods use the same rewrite
checkpoint, entity checkpoint, local Qwen3.8-27B SQL generator, database files,
and question IDs.

This is a tool- and retry-budget-controlled comparison, not an equal-total-token
comparison.  The adaptive method must additionally call its controller model;
the measured token cost is reported rather than hidden or padded on the fixed
baseline.

## Primary paired comparison: MaskSQL/BIRD 300

All values below are means over three fresh inference runs.  `±` is the sample
standard deviation across the three runs.

| Method | EX | Tool calls / case | SQL attempts / case | Tokens / case |
|---|---:|---:|---:|---:|
| One-pass fixed | 60.00 ± 0.33 | 6.00 | 1.000 | 1,015 |
| Iterative fixed | 60.78 ± 0.38 | 6.21 | 1.070 | 1,038 |
| Adaptive SFT | 67.00 ± 0.58 | 6.55 | 1.014 | 39,675 |
| Adaptive RL step-10 supplement | 65.33 ± 2.65 | 6.49 | 1.016 | 38,152 |

- Iterative fixed minus one-pass fixed: **+0.78 percentage points**, paired
  question-cluster bootstrap 95% CI **[-0.44, 2.11]**.
- Adaptive SFT minus iterative fixed: **+6.22 percentage points**, paired
  question-cluster bootstrap 95% CI **[1.22, 11.22]**.
- Adaptive RL minus iterative fixed: **+4.56 percentage points**, 95% CI
  **[-0.33, 9.44]**.  This supplement has visibly higher run-to-run variance,
  so it should not be used as the sole statistical claim.

The stronger fixed baseline therefore recovers less than one point over the
one-pass pipeline.  The SFT adaptive controller retains a 6.22-point advantage
despite using slightly fewer SQL-generation attempts (1.014 versus 1.070).

## Same-ID checks on additional datasets

These are single inference runs on the exact same 300 IDs per dataset.  The
confidence intervals resample paired questions; they do not represent
between-seed uncertainty.

| Dataset | Iterative fixed EX | Adaptive SFT EX | Difference | Paired bootstrap 95% CI |
|---|---:|---:|---:|---:|
| Spider dev (300) | 58.00 | 65.33 | +7.33 | [2.00, 12.67] |
| Spider test (300) | 66.67 | 80.67 | +14.00 | [9.00, 19.33] |

All 600 adaptive cases and all paired fixed cases completed without evaluator
or model-service failures.  The adaptive controller used 6.68 versus 6.19 tool
calls per case on Spider dev, and 5.89 versus 6.12 on Spider test.  Thus the EX
gain is not explained by more external tool calls or more SQL attempts.  It
does require substantially more controller-model tokens: about 43.1k versus
1.1k tokens per case on Spider dev and 40.5k versus 1.4k on Spider test.

## Full-dataset fixed-pipeline check

The one-pass and iterative fixed workflows were also run on every example.  EX
differences use only IDs for which the gold SQL executed successfully in both
runs.

| Dataset | Common valid IDs | One-pass EX | Iterative EX | Difference |
|---|---:|---:|---:|---:|
| BIRD dev | 1,485 | 55.96 | 56.09 | +0.13 |
| Spider dev | 1,032 | 57.27 | 57.85 | +0.58 |
| Spider test | 2,147 | 53.84 | 54.12 | +0.28 |

BIRD had load-sensitive gold-query timeouts (44 in the one-pass run and 34 in
the iterative run); Spider dev had two fixed gold-query failures in both runs;
Spider test had none.  These cases are recorded as infrastructure failures and
are excluded from the paired common-valid differences above.

## Interpretation boundaries

- These supplementary runs use the local Qwen3.8-27B SQL backend.  They should
  not be presented as a reproduction of the paper's cloud-backed headline EX.
- Privacy precision/recall in the JSON summaries measures whether annotated
  surface terms were masked.  It does not measure semantic or attribute
  inference leakage.
- The multi-dataset adaptive results use one run per Spider split.  The primary
  BIRD comparison is the one supported by three repeated runs.

## Rebuttal-ready paragraph

> We thank the reviewer for requesting a stronger non-agentic control. We added
> a deterministic iterative pipeline using the same query-rewrite, entity-
> extraction, masking, SQL-generation, unmasking, and read-only execution tools
> as PrivAgent-R1. The controller follows a fixed state machine and permits one
> additional SQL-generation attempt only after an execution failure; execution
> errors are re-masked before the retry. On the paired 300-question MaskSQL/BIRD
> set over three runs, one-pass fixed, iterative fixed, and adaptive SFT obtain
> 60.00%, 60.78%, and 67.00% EX, respectively. The retry adds only 0.78 points
> (95% CI: -0.44 to 2.11), whereas adaptive SFT remains 6.22 points above the
> iterative control (95% CI: 1.22 to 11.22). The iterative control actually
> makes more SQL-generation attempts on average (1.070 vs. 1.014), while total
> external tool calls remain close (6.21 vs. 6.55). On matched 300-question
> subsets of Spider dev and Spider test, adaptive SFT exceeds the same iterative
> control by 7.33 and 14.00 points, respectively. We report total token usage
> separately because the adaptive controller incurs additional model calls;
> accordingly, we describe this as a tool/retry-budget-controlled comparison,
> not an equal-total-token comparison.
