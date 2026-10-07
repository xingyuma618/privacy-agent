# 统一 Qwen3.8-27B 教师的重做实验发布

发布日期：2026-10-07，北京时间。首轮完成时间：2026-10-07 01:14:22。

代码目录：`experiments/qwen38_27b_20261004/`。该目录包含原运行的 `src/`、`configs/`、`scripts/`、`tests/`、环境依赖、性能实验记录与审核证据。运行代码与原实验相同，本次仅整理发布、增加数据打包/校验/恢复入口和文档，没有修复或改变合成筛选标准。

## 模型与方法

所有教师角色使用原路径名 Qwen3.8-27B 的本地权重，学生为 Qwen3-4B-Instruct-2507。教师隐藏 thinking 从首次调用即关闭；显式 ReAct Thought 仍由策略提示要求。输入为原始 BIRD train 的 9,428 个问题、69 个数据库，无旧 DeepSeek/Qwen2.5-72B 标注或轨迹筛选名单复用。

Prompt 直接合成一条轨迹；MCTS 使用 UCB1 系数 1.414，最多 24 次模拟、每次最多 3 个子节点，首次通过执行/隐私筛选即停止。每条路径最多 20 轮。4 个 vLLM TP=2 副本使用 8 卡，100 个共享样本线程按“标注 → Prompt → MCTS”处理每题，并非每种方法各 50 并发。两种方法生成预算不同，不能称为等计算量比较。

## 完整首轮结果

| 项目 | Prompt | MCTS |
| --- | ---: | ---: |
| 处理完成 | 9,428 | 9,428 |
| 原标准 accepted / 导出训练轨迹 | 3,547 | 5,417 |
| rejected | 5,306 | 3,382 |
| 标注或调用错误 | 575 | 629 |

另含 8,862 条结构有效实体标注/实体提取训练数据、6,337 条教师判断等价的改写训练数据。原标准 accepted 不等于最终合格；全部案例保留状态、完整工具消息、外发内容、SQL、评估和调用成本。结果来自 `runs/full_parallel100/summary.json`，日志中的耗时和 token 不能替代控制预算后的比较，共享标注成本与后端测速暂停也需单独处理。

## 数据位置与恢复

所有新数据采用可复现 gzip 压缩，共 35 个文件、151.11 MiB。最大压缩文件约 18.19 MiB。`data_manifest.json` 记录原始及存储校验和；`source_manifest.json` 记录代码/配置来源。模型权重、SQLite 数据库、运行环境和服务日志不在发布包中；原始 SQLite 数据库沿用历史发布所述的官方 BIRD 来源。

在仓库根目录执行：

```bash
python scripts/prepare_data.py --check-only
python scripts/verify_qwen38_release.py
python scripts/prepare_data.py
```

最后一个命令按 manifest 解压历史和新版数据，解压文件由 gitignore 排除；也可以直接用 `gzip.open()` 读取压缩数据而不解压。

新版目录下的数据：

- `data/bird_train.jsonl.gz`：问题、evidence、gold SQL、完整 schema、历史本机数据库路径；9,428 条。
- `data/input_manifest.json`：输入及教师配置校验和、权重文件清单。
- `runs/full_parallel100/prompt/train.jsonl.gz`、`mcts/train.jsonl.gz`：原标准下的学生训练消息。
- `runs/full_parallel100/entity_extract_train.jsonl.gz`、`query_rewrite_train.jsonl.gz`：工具训练数据。
- `runs/full_parallel100/cache_archives/labels-*.jsonl.gz`：全部标注、改写及判别缓存。
- `runs/full_parallel100/cache_archives/prompt-*.jsonl.gz`、`mcts-*.jsonl.gz`：每种方法全部 9,428 个案例，包括拒绝与错误。每片最多 1,000 条；每行完整保留一个案例的 JSON 值。
- `runs/full_parallel100/manifest.json`、`summary.json`、`cache_provenance.json`：原配置、结果及少量旧并发运行缓存复用来源。
- `audit/20261004_232200/`：固定早期快照的审核报告及证据，快照仅覆盖当时已处理的数据，不是全量审核。

恢复原逐案例缓存布局（可直接读 gzip 时不必执行）：

```bash
python scripts/restore_qwen38_cache.py
# 或恢复到单独目录进行检查
python scripts/restore_qwen38_cache.py --output /tmp/qwen38-cache
```

脚本校验存储与解压校验和、条数及重复键；已有不同内容不会覆盖。归档将 JSON 缩为一行，保留全部字段值，没有裁剪轨迹或成本记录。原始 fingerprint 仍保留。

## 当前已知问题

1. 硬隐私检查依赖教师标注，遗漏 evidence 中的原始列名时，打分模型也可能漏检。早期快照已确认两条进入训练导出的列名泄露。
2. 实体标注存在覆盖不足及组合列格式错误，结构有效不代表正确、完整。工具训练数据需补审。
3. 仅比较单个数据库的执行结果可能误接收空结果 SQL，已发现输出列交换及值大小写错误。
4. 某些数据库的 schema 与累计消息较长，触发 65,536 token 服务上下文上限；这些案例记录为调用错误。还需改进消息长度处理并重试。
5. 训练数据超过 8K 的轨迹较多，训练前需要确定长度策略和实际数据加载格式，不能无检查截断。

详见 [固定快照审核报告](../experiments/qwen38_27b_20261004/audit/20261004_232200/REPORT.md)。这些问题尚未修复，发布包不应被标记为 clean/final。新版学生 SFT 与 RL 尚未启动；仓库根目录原有训练入口仍对应历史数据，不会自动切换到这一批。

## 重新运行

保留原配置方便审计，其中模型和数据库路径是原机器绝对路径。在另一台机器重新运行时，应同时调整 `configs/synthesis.json` 与 `configs/synthesis_parallel100.json` 的模型、原始训练 JSON 和数据库路径，再重建输入。代码或配置变更应使用新的运行名称，不要将新结果混入历史缓存。

```bash
cd experiments/qwen38_27b_20261004
mkdir -p runs logs data
uv venv .venv --python /usr/bin/python
uv pip install --python .venv/bin/python 'vllm==0.17.1' 'torch==2.10.0' \
  'openai==2.24.0' 'httpx==0.28.1' 'sqlglot==30.21.0' 'pyyaml==6.0.3' --torch-backend=cu128
# 先按本机环境调整上述两个配置，并准备对应官方数据库及原始 train.json
.venv/bin/python src/prepare_inputs.py
.venv/bin/python scripts/start_services.py 4
.venv/bin/python scripts/start_synthesis.py --run reproduction_new
.venv/bin/python -m unittest discover -s tests -v
```

源码 `requirements.txt` 记录原 CUDA 12.8 环境版本。SGLang 对比脚本保留作为实验记录；稳定配置在该机器的测试中没有表现出速度优势，完整合成使用 vLLM。

本次发布检查覆盖数据格式/条数/校验和、逐案例缓存恢复和原 7 项单元测试，不代表已重新执行全量 SQL、独立隐私攻击评估或学生训练。
