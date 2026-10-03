
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[2]))

import pandas as pd

# 读取上传的 CSV 文件（注意：系统内部将其转化为了 CSV 格式）
file_path = str(_RELEASE_ROOT / 'privacy-agent/agent_data_create/metrics.csv')
df = pd.read_csv(file_path)

# 计算 F1 值： 2 * (P * R) / (P + R)
df['M-F1'] = 2 * (df['M-P'] * df['M-R']) / (df['M-P'] + df['M-R'])

# 打印前几行查看结果
print(df.head())

# 将计算结果保存为新的 CSV 文件
output_path = "metrics_with_f1.csv"
df.to_csv(output_path, index=False)
print(f"计算完成，已保存至 {output_path}")