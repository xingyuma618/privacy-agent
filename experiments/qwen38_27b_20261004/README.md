> 发布状态（2026-10-07）：首轮合成已完成，Prompt / MCTS 导出 3,547 / 5,417 条轨迹；筛选问题尚未修复，新版学生训练未启动。请先阅读 [新版发布说明](../../docs/QWEN38_RESTART.md) 和 [审核报告](audit/20261004_232200/REPORT.md)。以下保留原运行设置与记录。

# 隐私保护智能体：27B 教师重做实验

独立实验目录，原实验保留。数据合成、标注、问题改写、实体提取、SQL 教师、隐私打分统一使用本地 `/mnt/wh_intern/MXY/models/Qwen3.8-27B`；学生仍为 `Qwen3-4B-Instruct-2507`。本目录已完成首轮数据合成，尚未启动学生训练。

## 输入和两种合成方法

从原始 BIRD train 的 9,428 条问题及 69 个 SQLite 数据库重新构建完整 schema，不使用旧教师生成的过滤名单、标注或轨迹。原始数据和数据库路径见 `configs/synthesis_parallel100.json`。输入、代码和配置指纹随运行保存。

Prompt：教师按 ReAct 工具协议直接生成一条轨迹，温度 0.8，最多 20 轮。

MCTS：UCB1 系数 1.414，最多 24 次模拟、每次扩展最多 3 个子节点，首次找到执行及隐私有效轨迹停止。模拟只修改状态副本；搜索树保存前缀，评估相同结果使用缓存。

两种方法共用输入、独立标注和验收条件：最终 SQL 在本地只读数据库上与 gold SQL 执行结果一致；检查所有 SQL 生成请求的 query 和 schema；独立标注的敏感原文不得暴露，27B 隐私判别均须 score=1 且 leaks 为空。gold SQL 仅用于训练期标注和结果评估，不提供给策略或 SQL 教师。教师判断仍有误差，不能据此宣称严格隐私保证。

保留重复行的 SQL 比较、按表区分同名列、AST 方式恢复标识符和值。中途恢复的工具错误允许通过，错误记录完整保留，两种方法规则一致。多工具并发调用返回明确错误及完整工具响应协议。

## 运行

当前 `.venv` 指向 `/tmp/privacy-qwen38-runtime`（CUDA 12.8 的 torch 2.10.0 和 vLLM 0.17.1）。临时盘在机器重启后可能清空，需要重建环境。安装命令：

```bash
uv venv /tmp/privacy-qwen38-runtime --python /usr/bin/python
uv pip install --python /tmp/privacy-qwen38-runtime/bin/python 'vllm==0.17.1' 'torch==2.10.0' openai httpx sqlglot pyyaml --torch-backend=cu128
.venv/bin/python src/prepare_inputs.py
.venv/bin/python scripts/start_services.py 4
.venv/bin/python scripts/start_synthesis.py
.venv/bin/python scripts/status.py
tail -f logs/full_parallel100.log
```

2026-10-04 并发优化后，当前运行目录为 `runs/full_parallel100`，配置为 `configs/synthesis_parallel100.json`。原 `runs/full` 已暂停；完整结果按来源和原指纹复用，未完成搜索重新执行。源代码、采样 seed、教师权重、提示词和验收规则保持相同。超时延长至 1800 秒，仅影响请求等待。

每个服务最多 48 条生成序列，4 个服务合计 192 条（MCTS 的 n=3 调用会占用多个序列）。启用前缀缓存及解码 CUDA Graph，保持 BF16 和原 65,536 上下文上限。

4 个教师副本分别使用 GPU 0–1、2–3、4–5、6–7，端口 18270–18273，仅监听本机。没有学生训练并发占用这些 GPU。`scripts/stop_services.py` 可停止这些服务。

合成任务可断点续跑：同一代码和配置下，已完成样本跳过、基础设施错误重试；修改代码或配置需使用新的 `--run` 名称。每个问题两个方法顺序执行，问题之间并发 100 个工作线程。实际调用次数、时间、token 用量记录在样本缓存中；不声称两个方法生成预算相等。

## 产物

- `data/bird_train.jsonl`：重新构建的输入。
- `runs/full_parallel100/labels/`：27B 新标注、改写及等价性判别。
- `runs/full_parallel100/prompt/cases/` 和 `runs/full_parallel100/mcts/cases/`：所有接受、拒绝及失败轨迹。
- `runs/full_parallel100/prompt/train.jsonl` 和 `runs/full_parallel100/mcts/train.jsonl`：仅通过验收的冷启动训练消息。
- `runs/full_parallel100/entity_extract_train.jsonl`、`query_rewrite_train.jsonl`：工具训练数据，改写仅导出教师判定等价的样本。
- `runs/full_parallel100/summary.json`：每完成 100 个问题及结束时更新；首个完成问题也更新。
- `logs/full_parallel100.log`：每个问题完成时输出进度；`scripts/status.py` 读取实时缓存计数。

学生模型路径固定在配置中。合成完成后应按相同保留样本数比较两种冷启动训练，单独报告生成成本，并保留自动教师标签与人工复核标签的区别。

## 验证

```bash
.venv/bin/python -m unittest discover -s tests -v
```

覆盖搜索树状态独立、搜索继续扩展、所有外发请求审查、同名列及外键、重复行、值类型和引号恢复、只读 SQL。
