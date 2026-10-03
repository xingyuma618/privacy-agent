
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[3]))

import argparse
import yaml
import json
import os
from datetime import datetime
from typing import Any, Dict, List

from runner.run_manager import RunManager

def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Run the pipeline with the specified configuration.")
    parser.add_argument('--data_mode', type=str, required=True, help="Mode of the data to be processed.")
    parser.add_argument('--data_path', type=str, required=True, help="Path to the data file.")
    parser.add_argument('--config', type=str, required=True, help="Path to the configuration file.")
    parser.add_argument('--num_workers', type=int, default=2, help="Number of workers to use.")
    parser.add_argument('--log_level', type=str, default='warning', help="Logging level.")
    parser.add_argument('--pick_final_sql', type=bool, default=False, help="Pick the final SQL from the generated SQLs.")
    parser.add_argument('--sc_data_save_path', type=str, required=True, help="schema linking data save path")
    args = parser.parse_args()

    args.run_start_time = datetime.now().isoformat()
    with open(args.config, 'r') as file:
        args.config=yaml.safe_load(file)
    
    return args

def load_dataset(data_path: str) -> List[Dict[str, Any]]:
    """
    Loads the dataset from the specified path.

    Args:
        data_path (str): Path to the data file.

    Returns:
        List[Dict[str, Any]]: The loaded dataset.
    """
    with open(data_path, 'r') as file:
        dataset = json.load(file)
    return dataset

def get_existing_question_ids(output_file_path):
    """
    读取 .jsonl 文件，获取所有已经处理过的 question_id。
    """
    existing_ids = set()
    if not os.path.exists(output_file_path):
        print(f"输出文件不存在: {output_file_path}，将处理所有数据。")
        return existing_ids
        
    with open(output_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    data = json.loads(line)
                    if "question_id" in data:
                        existing_ids.add(data["question_id"])
                except json.JSONDecodeError:
                    continue
                    
    return existing_ids

def main():
    """
    Main function to run the pipeline with the specified configuration.
    """

    args = parse_arguments()
    dataset = load_dataset(args.data_path)
    # indices = [2992, 2999, 3010, 3011]
    # dataset = [dataset[i] for i in indices]
    output_path = str(_RELEASE_ROOT / 'privacy-agent/CHESS/output_data/spider_dev_sc.jsonl')
    existing_ids = get_existing_question_ids(output_path)
    dataset = [item for item in dataset if item.get('question_id') not in existing_ids]
    print(f"待重新处理的缺失数据量: {len(dataset)}")

    run_manager = RunManager(args)
    run_manager.initialize_tasks(dataset)
    run_manager.run_tasks()
    run_manager.generate_sql_files()

if __name__ == '__main__':
    main()
