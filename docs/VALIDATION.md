# 发布验证记录

整理日期：2026-10-04（Asia/Shanghai）。

完成的检查：

- 850 个 Python 文件通过 AST 语法检查；270 个 shell 文件通过 `bash -n`。
- 40 个发布数据文件通过压缩存储及解压后的 SHA-256 校验；36 个 JSON/JSONL 数据文件解析成功，JSONL 合计 64,834 行。
- LLaMA-Factory 6 个已注册项目数据集的本地文件存在。
- 本地 SQLite 内存库的建表、计数查询与 SQL markdown 提取 smoke check 通过。
- 发布内容扫描 API/GitHub token 及私钥模式，硬编码凭据已去除或改为环境变量。
- 暂存区检查包含所有 data_manifest 存储文件，排除解压后的重复大文件、`.env`、私钥、模型权重及 SQLite 数据库。

本次不包含 GPU SFT/GRPO 重训、LMDeploy/SGLang 服务集成测试或外部 API 评估；发布检查不能视为重现论文数值。数据版本及最终 rebuttal 缺失的划分见 DATA_LINEAGE.md。

## 2026-10-07：27B 教师实验追加发布

- `prepare_data.py --check-only` 校验全部 75 个历史与新增存储文件。
- 新增 35 个压缩数据文件均通过压缩前/后的 SHA-256、JSON/JSONL 解析、条数及缓存状态计数检查；共 151.11 MiB，最大单文件 18.19 MiB。
- Prompt/MCTS 案例键各 9,428 个，无重复且两种方法键集合一致；导出训练数据条数分别为 3,547/5,417，与首轮 summary 一致。
- 使用 `restore_qwen38_cache.py --output /tmp/privacy-qwen38-release-restore-check` 实际恢复 8,862 条标注及两种方法各 9,428 条案例，共 27,718 个 JSON 缓存文件，校验通过。
- 在发布副本中运行原 7 项测试，全部通过；新增发布脚本与实验 Python 文件通过 AST 检查。

这些检查确认发布完整性及代码的已有测试结果。早期审核已发现的隐私/标注/空结果问题仍存在，本次没有运行全量重新评分、补标或新版学生训练。详见 QWEN38_RESTART.md。
