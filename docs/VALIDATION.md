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
