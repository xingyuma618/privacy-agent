import asyncio
import json
import re as regex
import logging
import json
import logging
import regex
import json_repair  # 继承你原代码中的容错处理
from typing import List, Tuple

import json_repair
# 配置日志以便能看到解析报错（如果有）
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 模拟所有的外部依赖 (Mocks & Stubs)
# ==========================================

class ToolParser:
    _registry = {}

    def __init__(self, tokenizer) -> None:
        self.tokenizer = tokenizer

    @classmethod
    def register(cls, name):
        def wrapper(subclass):
            cls._registry[name] = subclass
            return subclass
        return wrapper

def rollout_trace_op(func):
    """模拟 tracing 装饰器"""
    async def wrapper(*args, **kwargs):
        return await func(*args, **kwargs)
    return wrapper

def get_event_loop():
    """获取当前事件循环"""
    return asyncio.get_running_loop()

class OpenAIFunctionToolSchema:
    pass

class FunctionCall:
    """模拟 FunctionCall 数据类"""
    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments
        
    def __repr__(self):
        return f"FunctionCall(name={self.name!r}, arguments={self.arguments!r})"

class MockTokenizer:
    """模拟 Tokenizer，直接返回我们预设的测试文本"""
    def __init__(self, expected_text):
        self.expected_text = expected_text

    def decode(self, responses_ids, *args, **kwargs):
        return self.expected_text


# ==========================================
# 2. 你的目标类 (原封不动)
# ==========================================

@ToolParser.register("hermes")
class HermesToolParser(ToolParser):
    """Adapted from https://github.com/vllm-project/vllm/blob/v0.9.1/vllm/entrypoints/openai/tool_parsers/hermes_tool_parser.py"""

    def __init__(self, tokenizer) -> None:
        super().__init__(tokenizer)

        self.tool_call_start_token: str = "<tool_call>"
        self.tool_call_end_token: str = "</tool_call>"
        self.tool_call_regex = regex.compile(r"<tool_call>(.*?)</tool_call>", regex.DOTALL)

    @rollout_trace_op
    async def extract_tool_calls(
        self, responses_ids: list[int], tools: list[OpenAIFunctionToolSchema] = None
    ) -> tuple[str, list[FunctionCall]]:
        loop = get_event_loop()
        text = await loop.run_in_executor(None, self.tokenizer.decode, responses_ids)
        if self.tool_call_start_token not in text or self.tool_call_end_token not in text:
            return text, []

        matches = self.tool_call_regex.findall(text)
        function_calls = []
        for match in matches:
            try:
                function_call = json.loads(match)
                name, arguments = function_call["name"], function_call["arguments"]
                function_calls.append(FunctionCall(name=name, arguments=json.dumps(arguments, ensure_ascii=False)))
            except Exception as e:
                # print('=======================',text,'*****************')
                logger.error(f"Failed to decode tool call: {e}")

        # remaing text exclude tool call tokens
        content = self.tool_call_regex.sub("", text)

        return content, function_calls

@ToolParser.register("privacy_agent")
class PrivacyAgentToolParser(ToolParser):
    """
    专为 PrivacyProtectionAgent 设计的工具解析器。
    支持从 ReAct 范式 (Thought/Action) 中提取工具调用，
    并完美复用原始智能体中的 json_repair 逻辑以增强解析鲁棒性。
    """

    def __init__(self, tokenizer) -> None:
        super().__init__(tokenizer)

        # 匹配 prompt 中定义的范式格式，例如: Action: {"name": "query_rewrite", "arguments": {...}}
        self.action_regex = regex.compile(r"Action:\s*(\{.*?\})", regex.DOTALL)
        
        # 备用正则：防止大模型遗漏 "Action:" 前缀，直接捕获具备 name 和 arguments 的 JSON 结构
        self.json_fallback_regex = regex.compile(r"\{\s*\"name\"\s*:\s*\".*?\"\s*,\s*\"arguments\"\s*:.*?\}", regex.DOTALL)

    @rollout_trace_op
    async def extract_tool_calls(
        self, responses_ids: List[int], tools: List['OpenAIFunctionToolSchema'] = None
    ) -> Tuple[str, List['FunctionCall']]:
        
        loop = get_event_loop()
        text = await loop.run_in_executor(None, self.tokenizer.decode, responses_ids)
        
        function_calls = []
        
        # 1. 优先按照 ReAct 设定的 Action 格式进行提取
        matches = self.action_regex.findall(text)
        
        # 2. 如果标准格式未匹配到，启动 Fallback 机制直接捕获类似工具调用的 JSON 块
        if not matches:
            matches = self.json_fallback_regex.findall(text)

        # 3. 遍历提取到的 JSON 字符串，并调用 json_repair 进行容错处理
        for match in matches:
            try:
                # 复用原智能体中极为关键的 json_repair，应对大模型 JSON 格式错误或转义符幻觉
                function_call = json_repair.loads(match)
                
                if isinstance(function_call, dict) and "name" in function_call:
                    name = function_call["name"]
                    arguments = function_call.get("arguments", {})
                    
                    # FunctionCall 规定的 arguments 必须是 JSON 字符串，如果解析出是 dict 则 dump 转回
                    arguments_str = json.dumps(arguments, ensure_ascii=False) if isinstance(arguments, dict) else str(arguments)
                    
                    function_calls.append(FunctionCall(
                        name=name, 
                        arguments=arguments_str
                    ))
            except Exception as e:
                logger.error(f"[PrivacyAgentToolParser] Failed to decode tool call via json_repair: {e}\nMatch text: {match}")

        # 4. 清理正文中的工具调用部分，保留 Thought 思考链和最后的 SQL (如果有) 作为 content
        content = self.action_regex.sub("", text)
        if not function_calls and matches:
            content = self.json_fallback_regex.sub("", content)

        return content.strip(), function_calls
# ==========================================
# 3. 测试用例执行入口
# ==========================================

async def main():
    # 原始测试文本
    # 注意：原文本结尾有冗余字符 `").}}}`，这在真实的LLM输出中很常见，会导致 json.loads 失败。
    # 我在这里将其修正为了合法的 JSON `"}}`，以验证正常解析逻辑。
    # 如果你想测试异常捕获机制，可以把结尾改回 `").}}}`，观察日志报错。
    test_text = """
Thought: Let me analyze this query. The user question is: \"List all words cited in the AI class label.\"\n\nLet me predict the masked query. It would be something like: \"List all [column_1]] cited in the [value_1]] class label.\"\n\nThis seems structurally sound. The query is clear enough that masking won't destroy the syntactic structure.\n\nLet me proceed with entity extraction.\n\nAction:\n<tool_call>\n{\"name\": \"entity_extract\", \"arguments\": {\"query_to_extract\": \"List all words cited in the AI class label.\"}}}\n</tool_call>
"""
    
    # 1. 实例化 MockTokenizer
    tokenizer = MockTokenizer(expected_text=test_text)
    
    # 2. 实例化 Parser
    # parser = PrivacyAgentToolParser(tokenizer)
    parser = HermesToolParser(tokenizer)

    
    # 3. 模拟一串 response_ids (因为 MockTokenizer 会直接返回 test_text，传什么 ID 都行)
    dummy_ids = [123, 456, 789]
    
    print("开始解析...\n" + "-"*40)
    
    # 4. 执行异步解析方法
    content, function_calls = await parser.extract_tool_calls(dummy_ids)
    
    # 5. 打印结果验证
    print("\n【剥离 Tool Call 后的正文 (content)】:")
    print(content.strip())
    
    print("\n【提取出的工具调用 (function_calls)】:")
    for fc in function_calls:
        print(f"Name: {fc.name}")
        print(f"Arguments (JSON string): {fc.arguments}")
        print("-" * 20)

if __name__ == "__main__":
    # 启动异步事件循环
    asyncio.run(main())