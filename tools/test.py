import os
from pprint import pprint
from typing import Dict, List, Any

# =========================================================
# 导入之前写好的两个类 (假设它们在同一个文件中，或者你自行 import)
from information_retriever import StandaloneInformationRetriever, DatabaseManagerInterface, StandaloneDatabaseManager
# =========================================================
# os.environ["OPENAI_API_KEY"] = "sk-xxxxxxxxxxxxxxxxx"
class MockDatabaseManager:
    """
    用于纯逻辑测试的 Mock 数据库管理器。
    无需真实的本地 sqlite 或 LSH 文件即可运行。
    """
    def get_db_schema(self) -> Dict[str, List[str]]:
        return {
            "users": ["user_id", "user_name", "credit_card_number", "age"],
            "transactions": ["transaction_id", "user_id", "amount", "date"]
        }

    def query_vector_db(self, query: str, top_k: int) -> Dict:
        # 模拟 Chroma 向量检索返回的列描述
        return {
            "users": {
                "credit_card_number": {
                    "column_name": "credit_card_number",
                    "column_description": "User's private credit card number",
                    "value_description": "16 digit string",
                    "score": 0.85
                }
            }
        }

    def query_lsh(self, keyword: str, signature_size: int, top_n: int) -> Dict:
        # 模拟 LSH 局部敏感哈希检索返回的真实数据库 Entity（实体值）
        if "John" in keyword:
            return {"users": {"user_name": ["John Doe", "John Smith"]}}
        return {}


def my_llm_keyword_extractor(question: str, hint: str) -> List[str]:
    """
    模拟 LLM 提取关键字的函数。
    在实际生产中，你可以把它替换为对 GPT-4 / 闭源大模型的 API 调用。
    """
    print(f"\n[Mock LLM] 正在分析问题和提示...")
    print(f" -> 问题: {question}")
    print(f" -> 提示: {hint}")
    # 假装大模型提取出了这三个关键词
    return ["John Doe", "credit_card_number", "transactions"]


def test_retriever_pipeline():
    """
    测试信息检索主流程
    """
    print("=== 1. 初始化数据库管理器 ===")
    
    # ---------------------------------------------------------
    # 【模式 A：使用 Mock 数据管理器 (推荐首次运行)】
    db_manager = MockDatabaseManager()
    
    # 【模式 B：使用真实的本地数据管理器】
    # 确保你设置了 OpenAI API Key，因为原代码中的列匹配依赖了 OpenAIEmbeddings
    
    db_manager = StandaloneDatabaseManager(
        db_directory_path="./data/dev_databases/spider", 
        db_id="spider"
    )
    # ---------------------------------------------------------

    print("=== 2. 初始化信息检索智能体 ===")
    # 这里我们为了能跑通 Mock 测试，将 retriever 中的 embedding 替换为 None 或 Dummy (原代码如果直接跑需要 OpenAI Key)
    retriever = StandaloneInformationRetriever(
        db_manager=db_manager,
        llm_keyword_extractor=my_llm_keyword_extractor
    )
    
    # 【注意】如果你使用 Mock 模式测试且没有配置 OpenAI API Key，为了防止 _get_similar_columns 报错，
    # 可以在测试时暂时把 Retriever 初始化中的 embedding_function 换成一个假的模型类。
    # 如果你本地配了环境变量，这步可以忽略。

    # 测试问题和提示 (含隐私数据 John Doe)
    test_question = "What is the credit card number and total transactions for John Doe?"
    test_hint = "Focus on the users and transactions tables, search for John Doe."

    print("\n=== 3. 执行核心检索逻辑 (retrieve_information) ===")
    try:
        # 这一步会依次执行: 关键字提取 -> 上下文检索 -> 相似列检索 -> 相似实体检索
        result = retriever.retrieve_information(
            question=test_question,
            hint=test_hint,
            top_k=2
        )
        
        print("\n================ 检索结果 ==================")
        
        print("\n🔍 1. 提取到的关键词 (Keywords):")
        pprint(result["keywords"])
        
        print("\n📖 2. 相关表和列描述 (Schema with Descriptions):")
        # 这里你可以看到隐私字段的 description 已经被检索出来了
        pprint(result["schema_with_descriptions"])
        
        print("\n🗂️ 3. 相似的列名 (Similar Columns):")
        pprint(result["similar_columns"])
        
        print("\n👤 4. 相似的实体值/Example Values (Schema with Examples):")
        # 这里你可以看到包含了真实的隐私内容 "John Doe"
        pprint(result["schema_with_examples"])
        
        print("\n================ 测试完成 ==================")
        print("💡 接下来，你可以将 result 传入你的「Schema 选择器」，并触发「隐私 Mask 处理工具」。")

    except Exception as e:
        print(f"\n[错误] 检索过程中发生异常: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_retriever_pipeline()