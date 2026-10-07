from __future__ import annotations
import re
from sqlglot import exp
from database import parse_query, render_schema

def contains(text, value):
    value = str(value)
    if not value:
        return False
    pattern = r'(?<!\w)' + re.escape(value) + r'(?!\w)'
    return re.search(pattern, text, re.IGNORECASE) is not None

def normalize_entities(entities, schema, query):
    if isinstance(entities, dict):
        entities = entities.get('entities', [])
    if not isinstance(entities, list):
        raise ValueError('Entities must be a JSON list.')
    output = []
    for entity in entities:
        if not isinstance(entity, dict):
            raise ValueError('Each entity must be a JSON object.')
        item = {k: entity.get(k, '') for k in ['entity', 'entity_type', 'table', 'column', 'cell_value']}
        if item['entity_type'] not in {'table', 'column', 'cell_value'} or not str(item['entity']).strip():
            raise ValueError('Invalid entity type or empty surface.')
        table = next((t for t in schema if t.casefold() == str(item['table']).casefold()), None)
        if table is None:
            raise ValueError(f"Unknown entity table: {item['table']}")
        item['table'] = table
        if item['entity_type'] != 'table':
            col = next((c['column_name'] for c in schema[table]['columns'] if c['column_name'].casefold() == str(item['column']).casefold()), None)
            if col is None:
                raise ValueError(f"Unknown column: {item['column']}")
            item['column'] = col
        if not contains(query, item['entity']):
            # Labels must refer to an actual surface, not an invented paraphrase.
            raise ValueError(f"Entity surface is absent from query: {item['entity']}")
        output.append(item)
    return output

def mask(query, entities, schema):
    entities = normalize_entities(entities, schema, query)
    tables = {t: f'table_{i+1}' for i, t in enumerate(schema)}
    columns = {(t, c['column_name']): f'col_{i+1}' for i, (t, c) in enumerate((t, c) for t, v in schema.items() for c in v['columns'])}
    values = {}
    numeric_values = {}
    substitutions = {}
    for entity in entities:
        kind = entity['entity_type']
        if kind == 'table':
            placeholder = tables[entity['table']]
        elif kind == 'column':
            placeholder = columns[(entity['table'], entity['column'])]
        else:
            original = str(entity['cell_value'])
            if not original:
                raise ValueError('Cell-value entity has no original value.')
            values.setdefault(original, f'val_{len(values)+1}')
            placeholder = values[original]
            datatype = next(c['data_type'] for c in schema[entity['table']]['columns'] if c['column_name'] == entity['column']).upper()
            numeric = any(t in datatype for t in ['INT', 'REAL', 'FLOA', 'DOUB', 'NUM', 'DEC']) and re.fullmatch(r'-?\d+(?:\.\d+)?', original) is not None
            numeric_values[placeholder] = numeric_values.get(placeholder, True) and numeric
        surface = str(entity['entity'])
        if surface.casefold() in substitutions and substitutions[surface.casefold()][1] != placeholder:
            raise ValueError(f'Ambiguous entity surface needs a query rewrite: {surface}')
        substitutions[surface.casefold()] = (surface, placeholder)
    # One pass prevents recursively replacing generated placeholders.
    ordered = sorted(substitutions.values(), key=lambda pair: len(pair[0]), reverse=True)
    if ordered:
        lookup = {surface.casefold(): placeholder for surface, placeholder in ordered}
        regex = re.compile('|'.join(r'(?<!\w)' + re.escape(surface) + r'(?!\w)' for surface, _ in ordered), re.IGNORECASE)
        masked_query = regex.sub(lambda match: lookup[match.group(0).casefold()], query)
    else:
        masked_query = query
    return {
        'masked_query': masked_query,
        'masked_schema_str': render_schema(schema, tables, columns),
        'unmask_dict': {'tables': {p: t for t, p in tables.items()}, 'columns': {p: [t, c] for (t, c), p in columns.items()}, 'values': {p: v for v, p in values.items()}, 'numeric_values': numeric_values},
    }

def unmask(sql, mapping):
    # Restore identifiers and literals using the parsed SQL, not string replacement.
    tree = parse_query(sql)
    table_map, column_map, values = mapping['tables'], mapping['columns'], mapping['values']
    aliases = {table.alias for table in tree.find_all(exp.Table) if table.alias}
    def transform(node):
        if isinstance(node, exp.Table) and node.name in table_map:
            node.set('this', exp.to_identifier(table_map[node.name], quoted=True))
        elif isinstance(node, exp.Column) and node.name in column_map:
            original_table, original_column = column_map[node.name]
            qualifier = node.table
            node.set('this', exp.to_identifier(original_column, quoted=True))
            if qualifier in table_map:
                node.set('table', exp.to_identifier(table_map[qualifier], quoted=True))
            elif not qualifier:
                # Preserve unqualified columns; qualifying across CTE/subquery scopes is unsafe.
                pass
            elif qualifier not in aliases:
                pass
        elif isinstance(node, exp.Literal) and node.is_string and node.this in values:
            original = values[node.this]
            node = exp.Literal.number(original) if mapping.get('numeric_values', {}).get(node.this, False) else exp.Literal.string(original)
        elif isinstance(node, exp.Column) and node.name in values:
            original = values[node.name]
            node = exp.Literal.number(original) if mapping.get('numeric_values', {}).get(node.name, False) else exp.Literal.string(original)
        return node
    return tree.transform(transform).sql(dialect='sqlite')
