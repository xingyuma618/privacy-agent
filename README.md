# Privacy-Agent / PrivAgent-R1

隐私保护 NL2SQL 智能体的代码与数据发布包。包含 schema linking、查询改写、敏感实体标注、工具 SFT、Prompt/MCTS 冷启动合成、Agent SFT、多轮 GRPO 强化学习，以及 SQL 执行与隐私攻击评估。

本仓库整理自原实验工程和早期可迁移包。保留训练框架的项目修改、工具实现、提示词、配置与必要数据；代码按原目录保留，避免拆分后破坏模块导入。

## 2026-10-07 更新：统一 27B 教师重做实验

新版代码和数据位于 [experiments/qwen38_27b_20261004](experiments/qwen38_27b_20261004/)。标注、改写、实体提取、策略、SQL 与打分统一使用本地 Qwen3.8-27B，学生为 Qwen3-4B-Instruct-2507。首轮 9,428 个 BIRD 问题已处理完成，原筛选标准下 Prompt / MCTS 分别导出 3,547 / 5,417 条轨迹，尚未开始新版学生训练。

**这批数据仍是待复核版本。** 已发现隐私漏检、标注覆盖不足、空结果误接收和上下文超限；发布不代表这些问题已修复，也不代表重现论文最终指标。包含全部接受、拒绝、失败案例、标注、工具训练数据与审核证据，详见 [新版发布说明](docs/QWEN38_RESTART.md)。

| 阶段 | 代码入口 |
| --- | --- |
| Schema linking / schema 构建 | `privacy-agent/CHESS/src/`, `privacy-agent/CHESS/run/` |
| 查询改写数据合成 | `privacy-agent/tools/query_rewrite/sql2nl.py` |
| 敏感实体自动标注 | `privacy-agent/tools/schema_linking/schema_linking.py` |
| 标注与改写数据转 SFT 格式 | `LlamaFactory/data/entity_extract/process.py`, `LlamaFactory/data/query_rewrite/process.py` |
| 工具模型训练 | `scripts/train_sft.sh entity`, `scripts/train_sft.sh query` |
| Prompt / MCTS 冷启动合成 | `scripts/synthesize_agent.sh`, `privacy-agent/agent_data_create/agent.py`, `mcts_data_synthesis_eraly_stop.py` |
| 冷启动对话格式转换 | `LlamaFactory/data/agent/mcts/process.py`, `LlamaFactory/data/agent/deepseek_pro/process.py` |
| Agent 冷启动 SFT | `scripts/train_sft.sh agent` 或 `agent_prompt` |
| RL 数据准备 | `scripts/preprocess_rl_data.sh`, `privacy-agent/verl/examples/data_preprocess/privacy_agent.py` |
| GRPO 训练与工具调用 | `scripts/train_rl.sh`, `privacy-agent/verl/verl/tools/privacy_tools.py` |
| 执行、隐私与长度奖励 | `privacy-agent/verl/verl/utils/reward_score/nl2sql.py` |
| RL checkpoint 合并 | `scripts/merge_rl_checkpoints.sh` |
| 推理与 EX / masking / RI 评估 | `scripts/eval_multi_checkpoint.sh`, `privacy-agent/agent_data_create/ineference/` |
| 固定流程与自适应控制对照 | `privacy-agent/emnlp_reviewer/matched_iterative_baseline_20261001/` |
| SFT / RL 对照 | `privacy-agent/emnlp_reviewer/sft_vs_rl_20261001/` |
| MCTS / Prompt 等样本对照 | `privacy-agent/kdd_reviewer/r2_matched_controls/` |
| 完整出站内容隐私审计 | `privacy-agent/kdd_reviewer/complete_outbound_privacy_audit.py` |

## 数据

`data_manifest.json` 列出 75 个数据文件的来源、条数、原始大小、存储大小和 SHA-256，包括历史 40 个文件及新版 35 个压缩文件。历史数据约 148 MiB，新版约 151 MiB；所有存储文件均小于 50 MiB。历史大文件及新版数据使用 gzip 存储。使用前在仓库根目录执行：

```bash
python scripts/prepare_data.py
```

脚本验证校验和并解压到原路径。解压文件被 `.gitignore` 排除。仅验证压缩包可执行 `python scripts/prepare_data.py --check-only`。

已包含工具 SFT 数据、Agent Prompt/MCTS 对话、BIRD/Spider schema 与问题、隐私标注、历史等样本对照数据和 RL parquet。SQLite 原始数据库和模型权重需要另行准备；数据库应使用对应的 [BIRD](https://bird-bench.github.io/) / [Spider](https://yale-lily.github.io/spider) 官方版本，并遵守各数据集条款。

**数据版本不能混用。** 此处历史 Prompt SFT 文件有 4,313 条，MCTS 有 3,101 条；旧等样本对照各 3,101 条。当前 rebuttal 描述的 Prompt 保留 2,913 条、各抽取 2,800 条实验，不能直接用这些历史文件冒充；该最终划分在本次整理范围内未找到。详见 [数据与实验来源](docs/DATA_LINEAGE.md)。

## 安装与配置

合成与评估、SFT、RL 推荐分别安装到独立 Python 环境，GPU 训练环境按各框架的依赖和 CUDA 版本配置：

```bash
pip install -r requirements.txt
# SFT environment
pip install -e './LlamaFactory[torch,metrics]'
# RL environment
pip install -e ./privacy-agent/verl
# Serving / rollout, in the appropriate environments
pip install lmdeploy sglang
```

各框架的 `pyproject.toml` / `requirements.txt` 随代码保留；根 `requirements.txt` 只覆盖合成和评估，不是完整 GPU 环境锁文件。

```bash
cp .env.example .env
# 编辑 .env，设置服务地址、模型名称及 API key
set -a
source .env
set +a
export BASE_MODEL=/path/to/Qwen3-4B-Instruct-2507
export BIRD_DB_BASE_DIR=/path/to/train_databases
export BIRD_DEV_DB_BASE_DIR=/path/to/dev_databases
```

密钥从环境变量读取；`.env` 不上传。默认 query rewrite / entity extract / SQL 服务端口为 8036 / 8037 / 8039。SQL 服务需自行部署模型或配置云端 OpenAI-compatible endpoint。

## 运行流程

现有训练数据可直接复用；重新生成数据的命令和输入约定见 [工作流说明](docs/WORKFLOW.md)。

```bash
# 1. 工具模型训练
bash scripts/train_sft.sh entity
bash scripts/train_sft.sh query
# 2. 启动两个工具模型；另行启动 SQL 模型服务
bash scripts/serve_auxiliary_models.sh
# 3. 可选：重新合成冷启动轨迹
bash scripts/synthesize_agent.sh mcts
# 4. Agent SFT：默认使用发布的历史 MCTS 训练数据
bash scripts/train_sft.sh agent
# 5. 用本机数据库路径重新生成 RL parquet
bash scripts/preprocess_rl_data.sh --db_base_dir "$BIRD_DB_BASE_DIR"
# 6. RL
export MODEL_PATH=/path/to/agent-sft-checkpoint
export EXP_NAME=privacy_agent_grpo
bash scripts/train_rl.sh
# 7. 合并与评估
bash scripts/merge_rl_checkpoints.sh
CHECKPOINT_ROOT=/path/to/merged-checkpoints bash scripts/eval_multi_checkpoint.sh
```

辅助模型服务在前台等待，需要另一个终端执行后续步骤。SFT YAML 保留历史超参数，包括 `cutoff_len: 500000`；可通过 `CUTOFF_LEN=8192 bash scripts/train_sft.sh agent` 明确覆盖，覆盖后的运行不应声称与历史训练参数相同。重合成轨迹之后需执行格式转换，再选择相应训练数据集。

RL 发布 parquet 含历史数据库路径，换机器必须重新运行预处理。隐私奖励现采用当前工程中启用隐私评分的实现；`ENABLE_PRIVACY_REWARD=0` 可运行执行奖励消融。早期复现包曾关闭隐私奖励，本发布默认不沿用该消融设置。

## 验证与来源

整理过程验证 Python/Bash 语法、数据校验和、JSON/JSONL 格式、注册数据集及本地 SQL 工具；本次没有重新运行 GPU 训练或调用云端模型复测论文指标。历史补充实验脚本保留其原有参数与服务约定，运行前须检查对应 README 和模型配置。

`source_manifest.json` 记录文件来自原工程还是早期可迁移包；[验证记录](docs/VALIDATION.md) 说明检查范围。第三方代码保留各目录的原 LICENSE；本仓库不为第三方数据、代码或模型重新授予许可，见 [第三方来源](THIRD_PARTY.md)。
