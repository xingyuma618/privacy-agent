from __future__ import annotations
import collections
import json
import sqlite3
import time
from pathlib import Path
import sqlglot
from sqlglot import exp

def quote(identifier):
    return '"' + identifier.replace('"', '""') + '"'

def connect(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=5)

def schema_from_sqlite(path):
    schema = {}
    with connect(path) as conn:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
        for (table,) in tables:
            columns = conn.execute(f'PRAGMA table_info({quote(table)})').fetchall()
            fks = conn.execute(f'PRAGMA foreign_key_list({quote(table)})').fetchall()
            schema[table] = {
                'columns': [{'column_name': c[1], 'data_type': c[2], 'pk_order': c[5]} for c in columns],
                'foreign_keys': [{'id': f[0], 'seq': f[1], 'table': f[2], 'from': f[3], 'to': f[4]} for f in fks],
            }
    return schema

def render_schema(schema, table_map=None, column_map=None):
    table_map = table_map or {t: t for t in schema}
    column_map = column_map or {(t, c['column_name']): c['column_name'] for t, v in schema.items() for c in v['columns']}
    result = []
    for table, info in schema.items():
        columns = info['columns']
        lines = [f"  {quote(column_map[(table, c['column_name'])])} {c['data_type'] or 'TEXT'}" for c in columns]
        pk = sorted([c for c in columns if c['pk_order']], key=lambda c: c['pk_order'])
        if pk:
            lines.append('  PRIMARY KEY (' + ', '.join(quote(column_map[(table, c['column_name'])]) for c in pk) + ')')
        groups = collections.defaultdict(list)
        for fk in info['foreign_keys']:
            groups[fk['id']].append(fk)
        for fks in groups.values():
            fks.sort(key=lambda f: f['seq'])
            ref = fks[0]['table']
            # SQLite table names are case-insensitive.
            ref = next((t for t in schema if t.casefold() == ref.casefold()), ref)
            if ref not in schema:
                continue
            ref_pk = sorted([c for c in schema[ref]['columns'] if c['pk_order']], key=lambda c: c['pk_order'])
            targets = [f['to'] or (ref_pk[i]['column_name'] if i < len(ref_pk) else None) for i, f in enumerate(fks)]
            if any((ref, target) not in column_map for target in targets):
                continue
            lines.append('  FOREIGN KEY (' + ', '.join(quote(column_map[(table, f['from'])]) for f in fks) + ') REFERENCES ' + quote(table_map[ref]) + '(' + ', '.join(quote(column_map[(ref, target)]) for target in targets) + ')')
        result.append('CREATE TABLE ' + quote(table_map[table]) + ' (\n' + ',\n'.join(lines) + '\n);')
    return '\n\n'.join(result)

def parse_query(sql):
    trees = sqlglot.parse(sql.strip(), read='sqlite')
    if len(trees) != 1 or not isinstance(trees[0], exp.Query):
        raise ValueError('Only one read-only SELECT/UNION query is allowed.')
    forbidden = (exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Drop, exp.Alter, exp.Command)
    if any(isinstance(node, forbidden) for node in trees[0].walk()):
        raise ValueError('Write statements are not allowed.')
    return trees[0]

def execute(path, sql, timeout=15, row_limit=100000):
    parse_query(sql)
    conn = connect(path)
    try:
        conn.execute('PRAGMA query_only=ON')
        deadline = time.monotonic() + timeout
        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        cursor = conn.execute(sql)
        rows = cursor.fetchmany(row_limit + 1)
        if len(rows) > row_limit:
            raise ValueError('Result exceeds row limit; cannot safely compare.')
        return rows, len(cursor.description or [])
    finally:
        conn.close()

def execution_match(path, predicted, gold):
    predicted_rows, predicted_cols = execute(path, predicted)
    gold_rows, gold_cols = execute(path, gold)
    if predicted_cols != gold_cols:
        return False
    if parse_query(gold).args.get('order') is not None:
        return predicted_rows == gold_rows
    return collections.Counter(predicted_rows) == collections.Counter(gold_rows)
