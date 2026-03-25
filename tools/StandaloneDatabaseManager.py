import os
import pickle
import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Any, Tuple
from datasketch import MinHash, MinHashLSH
from langchain_chroma import Chroma

from langchain_openai import OpenAIEmbeddings

class StandaloneDatabaseManager:
    """
    独立的数据管理类，实现了信息检索模块所需的所有底层数据接口。
    用于加载和查询 LSH 索引、Chroma 向量数据库以及 SQLite Schema。
    """
    def __init__(self, db_directory_path: str, db_id: str, embedding_function=None):
        """
        初始化数据库管理器
        :param db_directory_path: 数据库所在的根目录 (例如: /path/to/dev_databases/spider/ )
        :param db_id: 数据库的 ID (例如: spider)
        :param embedding_function: 向量模型实例，默认使用 OpenAIEmbeddings(model="text-embedding-3-small")
        """
        self.db_directory_path = Path(db_directory_path)
        self.db_id = db_id
        
        # 默认使用原代码中的 text-embedding-3-small
        self.embedding_function = embedding_function or OpenAIEmbeddings(model="/mnt/intern/MXY/models/bge-m3",openai_api_base="http://127.0.0.1:2335/v1",openai_api_key="EMPTY")
        
        self.sqlite_db_path = self.db_directory_path / f"{self.db_id}.sqlite"
        self.vector_db_path = self.db_directory_path / "context_vector_db"
        self.lsh_path = self.db_directory_path / "preprocessed" / f"{self.db_id}_lsh.pkl"
        self.minhashes_path = self.db_directory_path / "preprocessed" / f"{self.db_id}_minhashes.pkl"
        
        # 内部状态缓存
        self.lsh = None
        self.minhashes = None
        self.vector_db = None
        
        self._load_lsh()
        self._load_vector_db()

    def _load_lsh(self):
        """加载 LSH 和 MinHashes 字典"""
        try:
            with open(self.lsh_path, "rb") as file:
                self.lsh = pickle.load(file)
            with open(self.minhashes_path, "rb") as file:
                self.minhashes = pickle.load(file)
            logging.info(f"Successfully loaded LSH for {self.db_id}")
        except Exception as e:
            logging.error(f"Error loading LSH for {self.db_id}: {e}")
            self.lsh, self.minhashes = None, None

    def _load_vector_db(self):
        """加载 Chroma 向量数据库"""
        try:
            if self.vector_db_path.exists():
                self.vector_db = Chroma(
                    persist_directory=str(self.vector_db_path), 
                    embedding_function=self.embedding_function
                )
                logging.info(f"Successfully loaded Vector DB for {self.db_id}")
            else:
                logging.warning(f"Vector DB path not found: {self.vector_db_path}")
        except Exception as e:
            logging.error(f"Error loading Vector DB for {self.db_id}: {e}")
            self.vector_db = None

    # ==========================================
    # 接口 1: Schema 获取 (补全了 SQLite 真实解析)
    # ==========================================
    def get_db_schema(self) -> Dict[str, List[str]]:
        """
        连接实际的 SQLite 数据库，提取所有的表名和列名。
        :return: Dict[table_name, List[column_names]]
        """
        schema = {}
        if not self.sqlite_db_path.exists():
            logging.error(f"SQLite DB not found at {self.sqlite_db_path}")
            return schema

        try:
            conn = sqlite3.connect(self.sqlite_db_path)
            cursor = conn.cursor()
            
            # 获取所有表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            for table_name_tuple in tables:
                table_name = table_name_tuple[0]
                # 跳过 sqlite 系统表
                if table_name.startswith("sqlite_"):
                    continue
                
                # 获取该表的所有列名
                cursor.execute(f"PRAGMA table_info(`{table_name}`);")
                columns = cursor.fetchall()
                schema[table_name] = [col[1] for col in columns]
                
            conn.close()
        except Exception as e:
            logging.error(f"Failed to fetch schema from SQLite: {e}")
            
        return schema

    # ==========================================
    # 接口 2: Vector DB 上下文/列描述检索
    # ==========================================
    def query_vector_db(self, query: str, top_k: int) -> Dict[str, Dict[str, Dict[str, str]]]:
        """
        检索 Chroma 向量数据库中的相关 Schema 描述
        """
        if not self.vector_db:
            logging.warning("Vector DB is not loaded.")
            return {}

        table_description = {}
        try:
            relevant_docs_score = self.vector_db.similarity_search_with_score(query, k=top_k)
        except Exception as e:
            logging.error(f"Error executing vector DB query: {query}, Error: {e}")
            return {}
        
        for doc, score in relevant_docs_score:
            metadata = doc.metadata
            table_name = metadata.get("table_name", "").strip()
            original_column_name = metadata.get("original_column_name", "").strip()
            column_name = metadata.get("column_name", "").strip()
            column_description = metadata.get("column_description", "").strip()
            value_description = metadata.get("value_description", "").strip()
            
            if table_name not in table_description:
                table_description[table_name] = {}
            
            if original_column_name not in table_description[table_name]:
                table_description[table_name][original_column_name] = {
                    "column_name": column_name,
                    "column_description": column_description,
                    "value_description": value_description,
                    "score": score
                }
        
        return table_description

    # ==========================================
    # 接口 3: LSH 实体/数据值检索 (含内部依赖补全)
    # ==========================================
    def _create_minhash(self, signature_size: int, string: str, n_gram: int = 3) -> MinHash:
        """从字符串生成 MinHash (补全原代码中缺失的 preprocess 方法)"""
        m = MinHash(num_perm=signature_size)
        string = str(string).lower()
        for i in range(len(string) - n_gram + 1):
            m.update(string[i:i+n_gram].encode('utf8'))
        # 兼容长度小于 n_gram 的短字符串
        if len(string) < n_gram:
            m.update(string.encode('utf8'))
        return m

    def query_lsh(self, keyword: str, signature_size: int = 100, top_n: int = 10) -> Dict[str, Dict[str, List[str]]]:
        """
        使用 LSH 查询数据库中的相似实体值
        """
        if not self.lsh or not self.minhashes:
            logging.warning("LSH or Minhashes not loaded.")
            return {}

        n_gram = 3
        query_minhash = self._create_minhash(signature_size, keyword, n_gram)
        
        # 从 LSH 桶中获取候选结果
        results = self.lsh.query(query_minhash)
        
        # 计算 Jaccard 相似度并排序
        similarities = []
        for result in results:
            if result in self.minhashes:
                stored_minhash = self.minhashes[result][0]
                sim = query_minhash.jaccard(stored_minhash)
                similarities.append((result, sim))
                
        similarities = sorted(similarities, key=lambda x: x[1], reverse=True)[:top_n]

        # 整理返回结果
        similar_values_trimmed: Dict[str, Dict[str, List[str]]] = {}
        for result, similarity in similarities:
            # 原代码结构：self.minhashes[result] 是 (MinHash, table_name, column_name, value)
            _, table_name, column_name, value = self.minhashes[result]
            
            if table_name not in similar_values_trimmed:
                similar_values_trimmed[table_name] = {}
            if column_name not in similar_values_trimmed[table_name]:
                similar_values_trimmed[table_name][column_name] = []
                
            similar_values_trimmed[table_name][column_name].append(value)

        return similar_values_trimmed