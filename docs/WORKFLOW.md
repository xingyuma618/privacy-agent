# 工作流说明

以下命令在仓库根目录运行。先解压数据并加载 `.env`。原始模型、SQLite 数据库及 API 服务由运行者提供。

## Schema linking 与 schema 构建

`privacy-agent/CHESS/src/main.py`、`src/database_utils/` 与 `run/` 保留 CHESS 的原实现和提示词。`run/configs/` 配置 schema linking；数据库根路径、检索模型和服务凭据由 CHESS 配置或环境提供。发布的 `agent_data_create/data/*_sc.jsonl` 已包含预构建 schema，可直接用于后续合成；重建时检查 `CHESS/README.md` 与对应 run 配置。

## 查询改写数据合成

```bash
python scripts/prepare_data.py
export SQL2NL_DATA_PATH="$PWD/privacy-agent/tools/query_rewrite/bird.json"
export SQL2NL_OUTPUT_JSON="$PWD/privacy-agent/tools/query_rewrite/bird_sql2nl.json"
export SQL2NL_OUTPUT_JSONL="$PWD/privacy-agent/tools/query_rewrite/bird_sql2nl_ckpt.jsonl"
python privacy-agent/tools/query_rewrite/sql2nl.py
```

通过 `SYNTHESIS_BASE_URL`、`SYNTHESIS_MODEL_NAME`、`SYNTHESIS_API_KEY` 配置教师模型。输入含 `question`、`evidence`、`gold_sql`、`db_schema`；模板位于同目录 `sql2nl_template.py`。已有 `semantic_compare/bird-1.json` / `spider-1.json` 为历史语义筛选后的数据，语义筛选代码在 `semantic_compare/semantic.py`。

## 敏感实体标注与工具 SFT 数据

```bash
python privacy-agent/tools/schema_linking/schema_linking.py
# 用现有已筛选数据重新生成训练文件；会覆盖对应解压后的训练数据
python LlamaFactory/data/entity_extract/process.py
python LlamaFactory/data/query_rewrite/process.py
```

标注脚本的默认输入来自 `semantic_compare/bird-1.json`；输入/输出覆盖变量见脚本 `SCHEMA_LINKING_*` 配置。标注模型接收问题、schema 和 gold SQL；其自动标注不能视为人工金标。完整 BIRD MaskSQL 格式标注生成入口是 `agent_data_create/ineference/generate_bird_masksql_annotations.py`，用 `--help` 检查 CLI，显式传入需标注的全量数据。

`LlamaFactory/data/dataset_info.json` 注册 `query_rewrite`、`schema_linking`、`nl2sql_agent` 与 `nl2sql_agent_prompt`。工具模型 SFT 命令见根 README；`scripts/serve_auxiliary_models.sh` 以明确的 model alias 启动两个服务。

## Prompt / MCTS 冷启动

```bash
bash scripts/synthesize_agent.sh prompt
bash scripts/synthesize_agent.sh mcts
```

入口默认读取 `agent_data_create/data/bird_train_sc_filtered.jsonl`，使用 rewrite、extract、SQL 三个模型服务和本地数据库。`BIRD_DB_BASE_DIR`、`EVAL_DATA_BASE_DIR`、`AGENT_MCTS_OUTPUT_DIR`、`AGENT_MCTS_TRAIN_DATA_DIR` 可配置；Prompt 对应变量见 `agent_data_create/agent.py`。

MCTS 使用 UCB1（探索系数 1.414），调用处为 24 次 simulation、最多 3 个 children，并对隐私与 SQL 执行有效路径提前停止。合成中的 gold SQL/隐私标签用于训练过滤，不是部署时的独立隐私验证器。

合成结果是原始工具调用对话，需要转换为 LLaMA-Factory 的 tools 字段与 `<tool_call>` 格式：

```bash
python LlamaFactory/data/agent/mcts/process.py
# Prompt 转换脚本的输入路径应指向本次合成的 train_data 文件
python LlamaFactory/data/agent/deepseek_pro/process.py
```

转换脚本保留原始输入约定。若输出目录有改变，应修改其 input_file_path 或选择自己的转换输入，勿让 SFT 继续读取旧训练文件。

等样本对照可用 `kdd_reviewer/r2_matched_controls/prepare_matched_cold_start.py`：它保留全部 MCTS、用 seed=2026 抽取同数量 Prompt。这是历史 n3101 方案，不是最终 rebuttal 的 n2800 划分。

## RL

数据库位置改变时始终重新生成 parquet：

```bash
bash scripts/preprocess_rl_data.sh --db_base_dir "$BIRD_DB_BASE_DIR"
MODEL_PATH=/path/to/cold-start-checkpoint EXP_NAME=privacy_agent_grpo bash scripts/train_rl.sh
```

NL2SQL 启动脚本实际使用 `algorithm.adv_estimator=grpo`，虽包含历史 `gsm8k` 项目名称，但 reward dispatch 和 tools 是 NL2SQL 实现。默认 4 GPUs、16 rollouts；GPU 数量或模型并行修改须同时检查对应 Hydra 配置。

`privacy_tools.py` 实现查询改写、实体抽取、typed masking、SQL generation、unmask 与本地执行。`nl2sql.py` 汇总 SQL 执行、隐私、长度与轮次奖励。当前默认 `LAMBDA_EXEC=2.0`、`LAMBDA_PRIV=2.0`，通过 `PRIVACY_*` 配置隐私 reward 服务。`ENABLE_PRIVACY_REWARD=0` 为消融运行。

checkpoint 合并入口在 `verl/examples/nl2sql/merge_verl_fsdp_all.sh`；若是不同实验名，设置 `EXP_NAME` 并检查其 checkpoint 根目录。

## 推理与隐私评估

```bash
CHECKPOINT_ROOT=/path/to/merged-checkpoints bash scripts/eval_multi_checkpoint.sh
# 完整参数，包括数据库、数据集、SQL backend 和攻击模型
python privacy-agent/agent_data_create/ineference/agent_eval_multi_checkpoint_lmdeploy_token_usage_privacy_f1.py --help
```

评估默认以发布的 BIRD 数据和 `masksql_test.json` 对齐 subset；数据集、ground truth、难度元数据和 SQLite 必须使用同一 split。Spider 需显式配置对应数据库和官方评估元数据。`RUN_RI_ATTACK=1` 启用攻击；`evaluate_ri_attack.py` 保留恢复攻击与评分代码。

masked entity 的精确恢复、语义恢复、漏检实体和完整出站内容曝光是不同指标。`complete_outbound_privacy_audit.py` 用于完整出站审计；不能将其结果当成攻击恢复率，也不能将 MCTS 训练筛选当成部署时保护门。
