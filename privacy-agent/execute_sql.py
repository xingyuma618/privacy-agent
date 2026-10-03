
# Resolve paths relative to this release; override for external data roots.
from pathlib import Path as _ReleasePath
import os as _release_os
_RELEASE_ROOT = _ReleasePath(_release_os.environ.get("PRIVACY_AGENT_RELEASE_ROOT", _ReleasePath(__file__).resolve().parents[1]))

import sqlite3

def execute_sql(sql, db_path):

    with sqlite3.connect(db_path) as conn:
        # Connect to the database
        cursor = conn.cursor()
        res = cursor.execute(sql)
        
    return res.fetchall()

if __name__ == '__main__':
    sql = '''SELECT Employees.FirstName FROM Employees JOIN Sales ON Employees.EmployeeID = Sales.SalesPersonID WHERE Sales.Quantity > 500'''
    db_path = str(_RELEASE_ROOT / 'privacy-agent/CHESS/data/train/train_databases_1/sales/sales.sqlite')
    print(execute_sql(sql, db_path))

