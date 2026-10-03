# 数据与实验来源

主入口采用早期可迁移包的运行路径配置，并保留当前推理评估、审计与对照实验代码。`source_manifest.json` 的 source 均相对于原 `paper/privacy-agent` 目录；以 `LlamaFactory/data/` 开头的转换脚本来自同级 `paper/LlamaFactory`。

| 数据 | 条数 | 用途 |
| --- | ---: | --- |
| `LlamaFactory/data/query_rewrite/v2/query_rewrite_train.json` | 14,870 | 查询改写工具 SFT |
| `LlamaFactory/data/entity_extract/merged_schema_linking_sharegpt.json` | 14,870 | 实体抽取工具 SFT |
| `LlamaFactory/data/agent/mcts/bird_train_sc_filtered_mcts_with_tools.jsonl` | 3,101 | 历史 MCTS 冷启动 SFT |
| `LlamaFactory/data/agent/deepseek_pro/bird_train_sc_filtered_deepseek_pro_with_tools.jsonl` | 4,313 | 历史 Prompt 冷启动 SFT |
| `privacy-agent/kdd_reviewer/r2_matched_controls/generated/*synthesis_n3101.jsonl` | 各 3,101 | 历史等样本对照；选择规则见同目录 manifest |

当前 rebuttal 使用不同的 Prompt 保留数（2,913）和匹配训练数（2,800）。本次未定位到该最终版本的独立数据文件或配置，故保留可核验的历史数据并明确标注。历史 synthesis token 用量未记录，不能声称生成计算预算相同。

全量自动标注与人工修正子集也必须区分。本发布保留可定位的自动标注、MaskSQL 对齐数据及评分代码；未确认独立的 100 例人工修正版，因此不宣称能够直接复算该子集的 71%/81% 结果。

RL parquet 是早期发布包的训练/验证划分，保留其原始字节以便追溯。`extra_info` 中数据库路径需通过预处理脚本按本机位置重新生成。预处理使用 95%/5%、seed=42 的划分，与标准 benchmark 测试集不同。

项目目录中的 `ineference`、`mcts_data_synthesis_eraly_stop.py` 等原始拼写予以保留，以免破坏导入或入口。未发布模型权重、SQLite 数据库、机器缓存、重复 case logs、审稿草稿和包含凭据的历史 Git 提交。
