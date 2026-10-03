import sqlite3
import os
import json
import math
import re
import sys
from typing import Any, Dict, Optional

from func_timeout import func_timeout, FunctionTimedOut
from openai import OpenAI, APITimeoutError, APIConnectionError

from verl.tools.tools import calculate_privacy_reward


# =========================
# 基础解析函数
# =========================

def extract_resp_parameters(solution_str: str):
    """
    从完整轨迹中提取最后一次 sql_generate 调用使用的 masked_query。
    """
    tool_call_pattern = re.compile(r'<tool_call>\s*(.*?)\s*</tool_call>', re.DOTALL)
    matches = tool_call_pattern.findall(solution_str)

    last_masked_query = None

    for match in matches:
        try:
            call_data = json.loads(match)

            if call_data.get("name") == "sql_generate":
                arguments = call_data.get("arguments", {})
                if "masked_query" in arguments:
                    last_masked_query = arguments["masked_query"]

        except json.JSONDecodeError as e:
            # 仅捕获JSON解析错误，方便排查
            print(f"JSON decode error: {e} in match: {match}")
            continue
        except Exception as e:
            # 捕获其他可能的异常并记录，方便定位
            print(f"Unexpected error: {e} in match: {match}")
            continue

    return last_masked_query


def extract_json_from_markdown(text: str):
    """
    从字符串中提取 Markdown 格式的 JSON 代码块。
    """
    pattern = r"```json\s*(.*?)\s*```"
    matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)

    if len(matches) > 0:
        return matches[-1]

    return text


def clean_space(sql: str):
    """
    清理 SQL 中的多余空格。
    """
    if not isinstance(sql, str):
        return ""

    sql = re.sub(r'\s*\)', ')', sql)

    # 聚合函数括号需要连着
    sql = re.sub(r'min\s*\(\s*', 'min(', sql)
    sql = re.sub(r'max\s*\(\s*', 'max(', sql)
    sql = re.sub(r'sum\s*\(\s*', 'sum(', sql)
    sql = re.sub(r'avg\s*\(\s*', 'avg(', sql)
    sql = re.sub(r'count\s*\(\s*', 'count(', sql)

    sql = re.sub(r'MIN\s*\(\s*', 'MIN(', sql)
    sql = re.sub(r'MAX\s*\(\s*', 'MAX(', sql)
    sql = re.sub(r'SUM\s*\(\s*', 'SUM(', sql)
    sql = re.sub(r'AVG\s*\(\s*', 'AVG(', sql)
    sql = re.sub(r'COUNT\s*\(\s*', 'COUNT(', sql)

    # 非聚合函数，括号只删除左括号右边空格
    sql = re.sub(r'\(\s*', '(', sql)

    # 删除多余空格
    sql = re.sub(r' +', ' ', sql)
    sql = re.sub(r'\s*,', ',', sql)
    sql = re.sub(r'[;\s]+$', '', sql)

    return sql


def extract_code(text: str, language: str = 'sql'):
    """
    从模型输出中提取 SQL 代码。
    """
    if not isinstance(text, str):
        return ""

    sql = text

    if f'```{language}' in text:
        pattern = rf"```{language}(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)

        if len(matches) > 0:
            sql = matches[-1].replace('\n', ' ').strip()
            return clean_space(sql)

    sql = sql.replace('\n', ' ').strip()
    return clean_space(sql)


# =========================
# 格式与推理奖励
# =========================

def simple_reasoning_steps_reward(solution_str: str):
    """
    检查是否有清晰的分步推理。
    """
    pattern_1 = r"(Step \d+:|^\d+\.|\n-|\n\*|First,|Second,|Next,|Finally,)"
    matches_1 = re.findall(pattern_1, solution_str, re.DOTALL | re.MULTILINE)

    return min(1.0, len(matches_1) / 3)


def code_format_reward(solution_str: str):
    """
    检查是否满足指定输出格式。
    """
    pattern = rf"^<think>\n.*?\n</think>\n<answer>\n.*?```sql.+?```.*?\n</answer>$"

    if re.match(pattern, solution_str, re.DOTALL | re.MULTILINE):
        return 1.0

    return 0.0


# =========================
# SQL 执行奖励，带超时控制
# =========================

def execute_sql(predicted_sql: str, ground_truth: str, db_path: str):
    """
    执行预测 SQL 和标准 SQL，并比较执行结果。
    """
    conn = None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(predicted_sql)
        predicted_res = cursor.fetchall()

        cursor.execute(ground_truth)
        ground_truth_res = cursor.fetchall()

        if set(predicted_res) == set(ground_truth_res):
            return 1

        return 0

    finally:
        if conn is not None:
            conn.close()


def exec_reward(
    pred_sql: str,
    gold_sql: str,
    db_id: str,
    db_base_dir: str,
    timeout_sec: float = 20.0,
):
    """
    SQL 执行奖励。

    返回：
    - SQL 执行结果正确：1.2
    - SQL 可执行但结果错误：0.2
    - SQL 超时或报错：0.0
    """
    db_path = os.path.join(db_base_dir, db_id, f'{db_id}.sqlite')

    try:
        acc = func_timeout(
            timeout_sec,
            execute_sql,
            args=(pred_sql, gold_sql, db_path),
        )
        syntax = 1

    except KeyboardInterrupt:
        sys.exit(0)

    except FunctionTimedOut:
        acc = 0
        syntax = 0

    except Exception:
        acc = 0
        syntax = 0

    return 1.0 * acc + 0.2 * syntax


# =========================
# 轮次奖励与长度奖励
# =========================

def calculate_turn_reward(
    turn_count: int,
    min_ideal: int = 5,
    max_ideal: int = 15,
    decay_rate: float = 0.5,
) -> float:
    """
    计算对话轮次奖励：
    - < min_ideal: 0
    - min_ideal ~ max_ideal: 1
    - > max_ideal: 指数衰减
    """
    if turn_count < min_ideal:
        return 0.0

    if min_ideal <= turn_count <= max_ideal:
        return 1.0

    return math.exp(-decay_rate * (turn_count - max_ideal))


def calculate_length_reward(
    token_length: int,
    max_limit: int = 256,
    penalty_rate: float = 0.005,
) -> float:
    """
    计算单轮 Assistant 纯思考内容长度奖励。
    """
    if token_length <= max_limit:
        return 1.0

    return math.exp(-penalty_rate * (token_length - max_limit))


def evaluate_response_metrics(response_text: str, model_tokenizer) -> dict:
    """
    从完整 response 中提取各项轨迹指标，并计算归一化后的奖励。
    """
    # 1. 工具调用次数
    turn_count = len(re.findall(r'<tool_call>', response_text))
    reward_turns = calculate_turn_reward(turn_count)

    # 2. 以 tool_response 切分每一轮 assistant 文本
    raw_turns = re.split(
        r'<tool_response>.*?</tool_response>',
        response_text,
        flags=re.DOTALL,
    )

    turn_lengths = []
    turn_length_rewards = []

    for turn_text in raw_turns:
        # 剔除 tool_call 及其内部内容
        thought_text = re.sub(
            r'<tool_call>.*?</tool_call>',
            '',
            turn_text,
            flags=re.DOTALL,
        )

        # 剔除框架标记
        thought_text = re.sub(
            r'\b(user|assistant|Action:|Thought:|</?think>)\b',
            '',
            thought_text,
        )

        cleaned_text = thought_text.strip()

        if not cleaned_text:
            continue

        token_length = len(model_tokenizer.encode(cleaned_text))
        turn_lengths.append(token_length)

        reward = calculate_length_reward(token_length)
        turn_length_rewards.append(reward)

    if turn_length_rewards:
        normalized_length_reward = sum(turn_length_rewards) / len(turn_length_rewards)
    else:
        normalized_length_reward = 1.0

    return {
        "turn_count": turn_count,
        "reward_turns": reward_turns,
        "turn_lengths": turn_lengths,
        "reward_length_normalized": normalized_length_reward,
        "turn_length_rewards_detail": turn_length_rewards,
    }


# =========================
# 隐私奖励，带远程请求超时控制
# =========================

def safe_privacy_reward(
    client: OpenAI,
    model_name: str,
    original_query: str,
    masked_query: Optional[str],
    db_schema: str,
    timeout_sec: float = 40.0,
) -> float:
    """
    对 calculate_privacy_reward 加总超时控制。

    如果出现以下情况，直接返回 0.0：
    - 没有 masked_query
    - 隐私奖励计算超时
    - 远程服务连接失败
    - 远程服务请求超时
    - 返回值不是合法数值
    """
    if not masked_query:
        return 0.0

    try:
        score = func_timeout(
            timeout_sec,
            calculate_privacy_reward,
            args=(client, model_name, original_query, masked_query, db_schema),
        )

        score = float(score)

        if math.isnan(score) or math.isinf(score):
            return 0.0

        # 如果你的 calculate_privacy_reward 本身返回范围不是 [0, 1]，
        # 这里可以去掉裁剪。
        return max(0.0, min(1.0, score))

    except KeyboardInterrupt:
        sys.exit(0)

    except FunctionTimedOut:
        return 0.2

    except (APITimeoutError, APIConnectionError):
        return 0.2

    except Exception:
        return 0.0


# =========================
# 安全写日志
# =========================

def safe_write_jsonl(output_path: Optional[str], data: Dict[str, Any]):
    """
    安全写入 jsonl。写日志失败不影响训练。
    """
    if not output_path:
        return

    try:
        with open(output_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + '\n')
    except Exception:
        pass


def get_float_config(reward_kwargs: Dict[str, Any], key: str, default: float) -> float:
    """Read a numeric reward config from reward_kwargs or an upper-case env var."""
    value = reward_kwargs.get(key, os.environ.get(key.upper(), default))
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_bool_config(reward_kwargs: Dict[str, Any], key: str, default: bool) -> bool:
    """Read a boolean reward config from reward_kwargs or an upper-case env var."""
    value = reward_kwargs.get(key, os.environ.get(key.upper(), default))
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


# =========================
# compute_score 主体逻辑
# =========================

def compute_score(
    solution_str: str,
    ground_truth: str,
    extra_info: dict,
    db_base_dir: str,
    **reward_kwargs,
):
    """
    真正的奖励计算逻辑。

    reward_kwargs 可选参数：
    - exec_timeout: SQL 执行奖励超时时间，默认 20 秒
    - privacy_base_url: 隐私奖励模型服务地址
    - privacy_model_name: 隐私奖励模型名
    - privacy_api_key: API key，默认 EMPTY
    - privacy_request_timeout: 单次 OpenAI HTTP 请求超时时间，默认 30 秒
    - privacy_total_timeout: calculate_privacy_reward 总超时时间，默认 40 秒
    - privacy_max_retries: OpenAI 请求重试次数，默认 0
    - lambda_exec: SQL 执行奖励权重，默认 2.0
    - lambda_priv: 隐私奖励权重，默认 2.0
    - enable_privacy_reward: 是否调用隐私奖励模型，默认 True
    - output_path: 调试日志输出路径，默认 output.jsonl
    - save_solution_str: 是否把完整 solution_str 写入日志，默认 False
    """
    db_id = extra_info['db_id']
    db_schema = extra_info['db_schema']
    tokenizer = extra_info['tokenizer']

    question = extra_info.get('question', '')
    evidence = extra_info.get('evidence', '')
    original_query = f"{question} {evidence}".strip()

    # =========================
    # 超时参数
    # =========================
    exec_timeout = 20
    privacy_request_timeout = 60
    privacy_total_timeout = 120
    lambda_exec = get_float_config(reward_kwargs, "lambda_exec", 2.0)
    lambda_priv = get_float_config(reward_kwargs, "lambda_priv", 2.0)
    enable_privacy_reward = get_bool_config(reward_kwargs, "enable_privacy_reward", True)

    # =========================
    # 隐私奖励模型配置
    # =========================
    privacy_base_url = reward_kwargs.get(
        "privacy_base_url",
        os.environ.get("PRIVACY_BASE_URL", "https://api.deepseek.com/v1"),
    )
    privacy_model_name = reward_kwargs.get(
        "privacy_model_name",
        os.environ.get("PRIVACY_MODEL_NAME", "deepseek-chat"),
    )
    privacy_api_key = reward_kwargs.get("privacy_api_key", os.environ.get("PRIVACY_API_KEY", "EMPTY"))
    privacy_max_retries = int(reward_kwargs.get("privacy_max_retries", 0))

    client = OpenAI(
        api_key=privacy_api_key,
        base_url=privacy_base_url,
        timeout=privacy_request_timeout,
        max_retries=privacy_max_retries,
    )

    # =========================
    # 1. SQL 执行奖励
    # =========================
    pred_sql = extract_code(solution_str, language='sql')
    gold_sql = extract_code(ground_truth, language='sql')

    exec_score = exec_reward(
        pred_sql=pred_sql,
        gold_sql=gold_sql,
        db_id=db_id,
        db_base_dir=db_base_dir,
        timeout_sec=exec_timeout,
    )

    # =========================
    # 2. 隐私保护奖励
    # =========================
    masked_query = extract_resp_parameters(solution_str)

    if enable_privacy_reward and lambda_priv > 0:
        privacy_reward = safe_privacy_reward(
            client=client,
            model_name=privacy_model_name,
            original_query=original_query,
            masked_query=masked_query,
            db_schema=db_schema,
            timeout_sec=privacy_total_timeout,
        )
    else:
        privacy_reward = 0.0
    
    # =========================
    # 3. 轨迹长度和轮次奖励
    # =========================
    try:
        response_metrics = evaluate_response_metrics(solution_str, tokenizer)
        normalized_length_reward = float(response_metrics['reward_length_normalized'])
        reward_turns = float(response_metrics['reward_turns'])

    except Exception:
        response_metrics = {}
        normalized_length_reward = 0.0
        reward_turns = 0.0

    total_score = (
        lambda_exec * exec_score
        + lambda_priv * privacy_reward
        + normalized_length_reward
        + reward_turns
    )

    print(
        f"Execution Score=====: {exec_score}",
        f"privacy_reward====: {privacy_reward}",
        f"lambda_exec====: {lambda_exec}",
        f"lambda_priv====: {lambda_priv}",
        f"normalized_length_reward====: {normalized_length_reward}",
        f"reward_turns====: {reward_turns}",
    )

    # =========================
    # 调试日志
    # =========================
    output = {
        'db_id': db_id,
        'pred_sql': pred_sql,
        'gold_sql': gold_sql,
        'masked_query': masked_query,
        'total_score': total_score,
        'exec_score': exec_score,
        'privacy_reward': privacy_reward,
        'lambda_exec': lambda_exec,
        'lambda_priv': lambda_priv,
        'enable_privacy_reward': enable_privacy_reward,
        'normalized_length_reward': normalized_length_reward,
        'reward_turns': reward_turns,
        'response_metrics': response_metrics,
        'timeout_config': {
            'exec_timeout': exec_timeout,
            'privacy_request_timeout': privacy_request_timeout,
            'privacy_total_timeout': privacy_total_timeout,
        },
    }

    # if bool(reward_kwargs.get("save_solution_str", False)):
    output['solution_str'] = solution_str

    output_path = reward_kwargs.get("output_path", "output.jsonl")
    safe_write_jsonl(output_path, output)

    return float(total_score)
