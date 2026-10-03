import sqlite3

def query_department_count(db_path):
    try:
        # 连接到SQLite数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 定义SQL查询语句
        sql = '''
        SELECT name FROM head WHERE born_state != 'California' ;
        '''

        # 执行SQL语句
        cursor.execute(sql.rstrip(';'))

        # 获取查询结果
        result = cursor.fetchone()
        count = result[0] if result else 0

        print(f"部门中不在管理表中的部门数量为: {count}")

    except sqlite3.Error as e:
        print(f"数据库错误: {e}")

    finally:
        # 关闭数据库连接
        if conn:
            conn.close()

if __name__ == "__main__":
    db_path = '/root/data/nl2sql/spider/database/department_management/department_management.sqlite'
    query_department_count(db_path)
