
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[1]))

import json
import numpy as np
from tqdm import tqdm
import tiktoken

# Initialize tokenizer
# Using cl100k_base (OpenAI) as a fast proxy. 
# For exact Llama counts, replace with: 
from transformers import AutoTokenizer
tokenizer = AutoTokenizer.from_pretrained("/mnt/wh_intern/MXY/models/Qwen3-4B-Instruct")
# tokenizer = tiktoken.get_encoding("cl100k_base")

def get_token_count(text):
    """Returns the number of tokens in a given text."""
    # Removed the disallowed_special argument for Hugging Face
    return len(tokenizer.encode(text))

def process_jsonl(file_path):
    token_counts = []
    
    with open(file_path, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Processing JSONL"):
            if not line.strip():
                continue
                
            try:
                data = json.loads(line)
                text_to_tokenize = ""
                
                # 1. Extract text from all messages
                if "messages" in data:
                    for msg in data["messages"]:
                        # Add role and content
                        if "role" in msg:
                            text_to_tokenize += f"{msg['role']}\n"
                        if "content" in msg and msg["content"]:
                            text_to_tokenize += f"{msg['content']}\n"
                        # Handle tool calls/results if they exist as strings
                        if "tool_call_id" in msg:
                            text_to_tokenize += f"{msg['tool_call_id']}\n"
                            
                # 2. Extract text from tools definition
                if "tools" in data:
                    # 'tools' in your example is a stringified JSON array
                    text_to_tokenize += f"{data['tools']}\n"
                
                # Count tokens for this specific row
                count = get_token_count(text_to_tokenize)
                token_counts.append(count)
                
            except json.JSONDecodeError:
                print("Skipped invalid JSON line.")
                
    return token_counts

if __name__ == "__main__":
    # Replace with your actual file path
    FILE_PATH = str(_RELEASE_ROOT / 'LlamaFactory/data/agent/deepseek_pro/bird_train_sc_filtered_deepseek_pro_with_tools.jsonl')
    
    print(f"Calculating token statistics for: {FILE_PATH}...")
    counts = process_jsonl(FILE_PATH)
    
    if counts:
        print("\n--- Token Statistics ---")
        print(f"Total Rows:    {len(counts):,}")
        print(f"Average:       {np.mean(counts):.2f}")
        print(f"Maximum:       {np.max(counts):,}")
        print(f"Minimum:       {np.min(counts):,}")
        print(f"95th Percentile: {np.percentile(counts, 95):.0f}")
    else:
        print("No valid data found to process.")