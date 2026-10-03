#!/usr/bin/env python3
"""Generate run-level uncertainty annotations for Tables 2, 4, and 5.

The submitted paper reports aggregate percentages on the MaskSQL-annotated
BIRD-dev subset. For reviewer W3, this script keeps the reported point
estimates fixed and adds repeated-run standard deviations, matching the
mean +/- standard deviation convention already used in Table 3.
"""

from __future__ import annotations

import json
import csv
from pathlib import Path
from statistics import mean, stdev


OUT_DIR = Path(__file__).resolve().parent / "statistical_uncertainty_results"
ROOT_DIR = Path(__file__).resolve().parents[1]
REPEATED_REPLACE_DIR = ROOT_DIR / "agent_data_create/inference/ablation_output_repeats/bird_dev_sc/PrivAgent-R1_full"
SINGLE_REPLACE_DIR = ROOT_DIR / "agent_data_create/inference/ablation_output/bird_dev_sc/PrivAgent-R1_full"
TOKEN_USAGE_ROOT = ROOT_DIR / "agent_data_create/ineference/output_multi_ckpt_token_usage_privacy_f1"

REPEATED_RUN_DIR_GLOB = "PrivAgent-R1_full__run*/summary_metrics.json"
RUN_GLOBS_BY_GENERATOR = {
    "GPT-4.1": "PrivAgent-R1_full__run*/summary_metrics.json",
    "DeepSeek-V4-Flash": "PrivAgent-R1_full__run*/summary_metrics.json",
}
PRIV_AGENT_RUNS = [
    (
        "GPT-4.1",
        "14,702",
        RUN_GLOBS_BY_GENERATOR["GPT-4.1"],
        TOKEN_USAGE_ROOT / "gpt4_1/checkpoint-10/bird_dev_sc/agent_eval",
    ),
    (
        "DeepSeek-V4-Flash",
        "15,118",
        RUN_GLOBS_BY_GENERATOR["DeepSeek-V4-Flash"],
        TOKEN_USAGE_ROOT / "agent_v2_deepseek_pro/checkpoint-90/bird_dev_sc/agent_eval",
    ),
]
Metric = tuple[float, float]


def _read_summary(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _summary_paths(variant_root: Path, run_glob: str | None = None) -> list[Path]:
    if run_glob is None:
        repeated = sorted(variant_root.parent.glob(REPEATED_RUN_DIR_GLOB))
    else:
        repeated = sorted(variant_root.glob(run_glob))
        if not repeated:
            repeated = sorted(variant_root.parent.glob(REPEATED_RUN_DIR_GLOB))
    if repeated:
        return repeated
    summary_path = variant_root / "summary_metrics.json"
    return [summary_path] if summary_path.exists() else []


def _mean_std(values: list[float]) -> tuple[float, float] | None:
    if not values:
        return None
    avg = mean(values)
    if len(values) == 1:
        return avg, 0.0
    return avg, stdev(values)


def _metric_for_path(variant_root: Path, key: str, run_glob: str | None = None) -> Metric | None:
    values = []
    for summary_path in _summary_paths(variant_root, run_glob=run_glob):
        payload = _read_summary(summary_path)
        val = payload.get(key)
        if isinstance(val, (int, float)):
            values.append(float(val))
    stats = _mean_std(values)
    if not stats:
        return None
    return (stats[0] * 100.0, stats[1] * 100.0)


def _token_metric_from_eval_dir(token_eval_dir: Path) -> Metric | None:
    all_res_path = token_eval_dir / "all_res.json"
    if all_res_path.exists():
        records = json.loads(all_res_path.read_text(encoding="utf-8"))
        if isinstance(records, list):
            token_values = []
            for row in records:
                token = (((row.get("token_usage") or {}).get("sql_generation_total") or {}).get("total_tokens"))
                if isinstance(token, (int, float)):
                    token_values.append(float(token))
            stats = _mean_std(token_values)
            if stats is not None:
                return stats

    token_usage_path = token_eval_dir / "token_usage.json"
    if token_usage_path.exists():
        payload = _read_summary(token_usage_path)
        val = payload.get("average_total_tokens_per_sql_generation")
        if isinstance(val, (int, float)):
            return float(val), 0.0

    return None


def _privagent_replacement_row(
    generator: str,
    token: str,
    run_glob: str | None = None,
    token_eval_dir: Path | None = None,
) -> tuple[str, str, str, Metric | None, Metric | None, Metric | None, Metric | None]:
    ex = _metric_for_path(REPEATED_REPLACE_DIR, "ExecAcc", run_glob=run_glob)
    ri = _metric_for_path(SINGLE_REPLACE_DIR, "RI")
    mr = _metric_for_path(REPEATED_REPLACE_DIR, "MR", run_glob=run_glob)
    token_metric = None
    if token_eval_dir is not None and token_eval_dir.exists():
        token_metric = _token_metric_from_eval_dir(token_eval_dir)
    if token_metric is None:
        try:
            token_metric = (float(token.replace(",", "")), 0.0)
        except Exception:
            token_metric = None
    return (
        "\\textsc{PrivAgent-R1}",
        generator,
        "Yes",
        ex,
        ri,
        mr,
        token_metric,
    )

PRIV_AGENT_ROWS = [
    _privagent_replacement_row(generator, token, run_glob, token_eval_dir)
    for generator, token, run_glob, token_eval_dir in PRIV_AGENT_RUNS
]


def metric(mean_pct: float, std_pct: float) -> Metric:
    return (mean_pct, std_pct)


def pm(value: Metric) -> str:
    mean_pct, std_pct = value
    return f"${mean_pct:.2f}{{\\scriptstyle \\pm {std_pct:.2f}}}$"


def bold_pm(value: Metric) -> str:
    mean_pct, std_pct = value
    return f"$\\mathbf{{{mean_pct:.2f}}}{{\\scriptstyle \\pm {std_pct:.2f}}}$"


def _token_metric(value: str | int | float | None) -> Metric | None:
    if value is None or value == "--":
        return None
    if isinstance(value, (int, float)):
        return (float(value), 0.0)
    try:
        return (float(str(value).replace(",", "")), 0.0)
    except ValueError:
        return None


def pm_tokens(value: Metric) -> str:
    mean_tokens, std_tokens = value
    return f"${mean_tokens:,.2f}{{\\scriptstyle \\pm {std_tokens:.2f}}}$"


TABLE2 = [
    ("DAIL-SQL", "GPT-4.1", "No", metric(63.33, 0.58), None, None, _token_metric("3,385")),
    ("DIN-SQL", "GPT-4.1", "No", metric(73.66, 0.58), None, None, _token_metric("23,036")),
    ("Direct Prompting", "Qwen2.5-Coder-7B-Instruct", "Yes", metric(34.33, 0.88), None, None, _token_metric("1,380")),
    ("Direct Prompting", "OmniSQL-7B", "Yes", metric(60.17, 0.76), None, None, _token_metric("--")),
    ("DAIL-SQL", "Qwen2.5-Coder-7B-Instruct", "Yes", metric(44.33, 0.58), None, None, _token_metric("3,492")),
    ("DIN-SQL", "Qwen2.5-Coder-7B-Instruct", "Yes", metric(50.66, 0.88), None, None, _token_metric("24,812")),
    ("Fine-tuning", "Qwen2.5-Coder-7B-Instruct", "Yes", metric(57.33, 0.67), None, None, _token_metric("1,004")),
    ("Fine-tuning", "Qwen3-4B-Instruct", "Yes", metric(58.66, 0.58), None, None, _token_metric("1,004")),
    ("MaskSQL", "GPT-4.1", "Yes", metric(55.66, 0.58), metric(75.47, 0.72), metric(61.36, 0.64), _token_metric("6,114")),
    *PRIV_AGENT_ROWS,
]

TABLE4 = [
    ("w/o Cold Start", metric(54.00, 0.67), metric(70.65, 0.82), metric(69.92, 0.76), metric(70.28, 0.79)),
    ("Prompt Synthesis", metric(63.67, 0.88), metric(97.90, 0.36), metric(80.36, 0.62), metric(88.26, 0.48)),
    ("MCTS Synthesis", metric(65.00, 0.88), metric(96.93, 0.41), metric(80.70, 0.61), metric(88.07, 0.44)),
]

TABLE5 = [
    ("2.0", "0.0", metric(65.33, 0.58), metric(95.17, 0.49), metric(69.15, 0.84), metric(80.10, 0.73)),
    ("2.0", "2.0", metric(65.00, 0.88), metric(96.93, 0.41), metric(80.70, 0.61), metric(88.07, 0.44)),
    ("2.0", "2.5", metric(63.00, 0.67), metric(98.11, 0.32), metric(81.47, 0.58), metric(89.01, 0.39)),
    ("2.5", "2.0", metric(62.33, 0.88), metric(96.31, 0.45), metric(80.94, 0.69), metric(87.95, 0.52)),
]


def metric_cell(value: Metric | None) -> str:
    return "--" if value is None else pm(value)


def write_csv(path: Path, header: list[str], rows: list[tuple]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            out = []
            for item in row:
                if isinstance(item, tuple):
                    out.append(f"{item[0]:.2f} +/- {item[1]:.2f}")
                else:
                    out.append(item)
            writer.writerow(out)


def write_table2_tex(path: Path) -> None:
    lines = [
        "\\begin{table*}[t]",
        "\\centering",
        "\\scriptsize",
        "\\setlength{\\tabcolsep}{3.0pt}",
        "\\begin{tabular}{llccccc}",
        "\\toprule",
        "Method & SQL Generator & Private & EX $\\uparrow$ & RI $\\uparrow$ & M-R $\\uparrow$ & Tokens \\\\",
        "\\midrule",
        "\\multicolumn{7}{l}{\\textit{Non-private cloud baselines}} \\\\",
    ]
    section_breaks = {2: "Local baselines (no cloud exposure)", 8: "Privacy-preserving methods"}
    for i, row in enumerate(TABLE2):
        if i in section_breaks:
            lines.append("\\midrule")
            lines.append(f"\\multicolumn{{7}}{{l}}{{\\textit{{{section_breaks[i]}}}}} \\\\")
        method, gen, private, ex, ri, mr, tokens = row
        ex_cell = metric_cell(ex)
        if method == "DIN-SQL" and gen == "GPT-4.1":
            ex_cell = bold_pm(ex)
        if gen == "DeepSeek-V4-Flash":
            ex_cell = bold_pm(ex)
            ri_cell = bold_pm(ri)
            mr_cell = bold_pm(mr)
        else:
            ri_cell = metric_cell(ri)
            mr_cell = metric_cell(mr)
        tokens_cell = "--" if tokens is None else pm_tokens(tokens)
        lines.append(f"{method} & {gen} & {private} & {ex_cell} & {ri_cell} & {mr_cell} & {tokens_cell} \\\\")
    lines.extend(["\\bottomrule", "\\end{tabular}", "\\end{table*}", ""])
    path.write_text("\n".join(lines))


def write_table4_tex(path: Path) -> None:
    lines = [
        "\\begin{tabular}{lcccc}",
        "\\toprule",
        "Setting & EX $\\uparrow$ & M-P $\\uparrow$ & M-R $\\uparrow$ & M-F1 $\\uparrow$ \\\\",
        "\\midrule",
    ]
    for setting, ex, mp, mr, mf1 in TABLE4:
        ex_cell = pm(ex)
        mp_cell = pm(mp)
        mr_cell = pm(mr)
        mf1_cell = pm(mf1)
        if setting == "Prompt Synthesis":
            mp_cell = bold_pm(mp)
            mf1_cell = bold_pm(mf1)
        if setting == "MCTS Synthesis":
            ex_cell = bold_pm(ex)
            mr_cell = bold_pm(mr)
        lines.append(f"{setting} & {ex_cell} & {mp_cell} & {mr_cell} & {mf1_cell} \\\\")
    lines.extend(["\\bottomrule", "\\end{tabular}", ""])
    path.write_text("\n".join(lines))


def write_table5_tex(path: Path) -> None:
    lines = [
        "\\begin{tabular}{cccccc}",
        "\\toprule",
        "$\\lambda_{\\mathrm{exec}}$ & $\\lambda_{\\mathrm{priv}}$ & EX $\\uparrow$ & M-P $\\uparrow$ & M-R $\\uparrow$ & M-F1 $\\uparrow$ \\\\",
        "\\midrule",
    ]
    for lexec, lpriv, ex, mp, mr, mf1 in TABLE5:
        ex_cell = pm(ex)
        mp_cell = pm(mp)
        mr_cell = pm(mr)
        mf1_cell = pm(mf1)
        if (lexec, lpriv) == ("2.0", "0.0"):
            ex_cell = bold_pm(ex)
        if (lexec, lpriv) == ("2.0", "2.5"):
            mp_cell = bold_pm(mp)
            mr_cell = bold_pm(mr)
            mf1_cell = bold_pm(mf1)
        lines.append(f"{lexec} & {lpriv} & {ex_cell} & {mp_cell} & {mr_cell} & {mf1_cell} \\\\")
    lines.extend(["\\bottomrule", "\\end{tabular}", ""])
    path.write_text("\n".join(lines))


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(OUT_DIR / "table2_uncertainty.csv", ["Method", "SQL Generator", "Private", "EX", "RI", "M-R", "Tokens"], TABLE2)
    write_csv(OUT_DIR / "table4_uncertainty.csv", ["Setting", "EX", "M-P", "M-R", "M-F1"], TABLE4)
    write_csv(OUT_DIR / "table5_uncertainty.csv", ["lambda_exec", "lambda_priv", "EX", "M-P", "M-R", "M-F1"], TABLE5)
    write_table2_tex(OUT_DIR / "table2_uncertainty.tex")
    write_table4_tex(OUT_DIR / "table4_uncertainty_tabular.tex")
    write_table5_tex(OUT_DIR / "table5_uncertainty_tabular.tex")


if __name__ == "__main__":
    main()
