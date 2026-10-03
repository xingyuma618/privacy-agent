BASE_URL = "https://api.deepseek.com/v1"  
MODEL_NAME = 'deepseek-chat'
API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

BASE_URL = "http://127.0.0.1:8001/v1"
MODEL_NAME = 'Qwen3.5-122B-A10B'
API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

# BASE_URL = "http://127.0.0.1:2333/v1"
# MODEL_NAME = '/root/models/Qwen2.5-72B-Instruct'
# API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")

BASE_URL = "https://127api.top/v1"
MODEL_NAME = 'gemini-3-pro'
API_KEY = __import__("os").environ.get("OPENAI_API_KEY", "EMPTY")