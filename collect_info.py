from decimal import Decimal
import json
import psycopg2


class PgJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(PgJsonEncoder, self).default(o)


def fetchall_as_list_of_dict(cur):
    result = list()
    rows = cur.fetchall()
    description = cur.description
    for row in rows:
        row_as_dict = dict()
        for index, col in enumerate(description):
            row_as_dict[col.name] = row[index]
        result.append(row_as_dict)
    return result


def quoted_and_comma_separated(elements):
    return ', '.join(map(lambda elem: "'" + elem + "'", elements))


def get_database_description(cur, database):
    request = '''
       SELECT pg_catalog.shobj_description(oid, 'pg_database') as comment
         FROM pg_catalog.pg_database
        WHERE datname = %(database)s
    '''
    cur.execute(request, {'database': database})
    rows = fetchall_as_list_of_dict(cur)
    if rows:
        return rows[0]['comment']
    return None


def regex_from_json(json_value, default):
    if json_value is None:
        return default
    elif isinstance(json_value, list):
        return ''.join(json_value)
    return json_value


def get_schemas(cur, schemas_whitelist_regex, schemas_blacklist_regex):
    schemas_whitelist_regex = regex_from_json(schemas_whitelist_regex, '^')
    schemas_blacklist_regex = regex_from_json(schemas_blacklist_regex, '^$')
    request = '''
       SELECT nspname as namespace
         FROM pg_catalog.pg_namespace
        WHERE nspname ~ %(schemas_whitelist_regex)s
          AND nspname !~ %(schemas_blacklist_regex)s
    '''
    cur.execute(request, {'schemas_whitelist_regex': schemas_whitelist_regex,
                          'schemas_blacklist_regex': schemas_blacklist_regex})
    rows = fetchall_as_list_of_dict(cur)
    result = list()
    for row in rows:
        result.append(row['namespace'])
    return result


def get_tables(cur, schema, tables_whitelist_regex, tables_blacklist_regex):
    tables_whitelist_regex = regex_from_json(tables_whitelist_regex, '^')
    tables_blacklist_regex = regex_from_json(tables_blacklist_regex, '^$')
    request = '''
       SELECT nspname as namespace
            , relname as tablename
            , pg_catalog.pg_get_userbyid(relowner) AS tableowner
            , pg_class.oid
            , pg_catalog.obj_description(pg_class.oid, 'pg_class') as table_description
            , relacl
            , CASE
              WHEN relkind = 'f' THEN
                'foreign table'
              WHEN relkind = 'm' THEN
                'materialized view'
              WHEN relkind = 's' THEN
                'special'
              WHEN relkind = 'r' THEN
                'table'
              ELSE
                'view'
              END as reltype
            , CASE
              WHEN relkind IN ('m', 'v') THEN
                pg_get_viewdef(pg_class.oid)
              ELSE
                NULL
              END as view_definition
         FROM pg_catalog.pg_class
         JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
        WHERE relkind IN ('f', 'm', 's', 'r', 'v')
          AND nspname = %(schema)s
          AND relname ~ %(tables_whitelist_regex)s
          AND relname !~ %(tables_blacklist_regex)s
    '''
    cur.execute(request, {'schema': schema,
                          'tables_whitelist_regex': tables_whitelist_regex,
                          'tables_blacklist_regex': tables_blacklist_regex})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_statistics(cur, table_oid):
    request = '''
       SELECT table_len
            , tuple_count
            , tuple_len
            , CAST(tuple_percent AS numeric(20,2)) AS tuple_percent
            , dead_tuple_count
            , dead_tuple_len
            , CAST(dead_tuple_percent AS numeric(20,2)) AS dead_tuple_percent
            , CAST(free_space AS numeric(20,2)) AS free_space
            , CAST(free_percent AS numeric(20,2)) AS free_percent
         FROM pgstattuple(CAST(%(table_oid)s AS oid));
    '''
    cur.execute(request, {'table_oid': table_oid})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_columns(cur, attrelid):
    # - uses pg_class.oid
    request = '''
       SELECT attname as column_name
            , attlen as column_length
            , CASE
              WHEN pg_type.typname = 'int4'
                   AND EXISTS (SELECT TRUE
                                 FROM pg_catalog.pg_depend
                                 JOIN pg_catalog.pg_class ON (pg_class.oid = objid)
                                WHERE refobjsubid = attnum
                                  AND refobjid = attrelid
                                  AND relkind = 'S') THEN
                'serial'
              WHEN pg_type.typname = 'int8'
                   AND EXISTS (SELECT TRUE
                                 FROM pg_catalog.pg_depend
                                 JOIN pg_catalog.pg_class ON (pg_class.oid = objid)
                                WHERE refobjsubid = attnum
                                  AND refobjid = attrelid
                                  AND relkind = 'S') THEN
                'bigserial'
              ELSE
                pg_catalog.format_type(atttypid, atttypmod)
              END as column_type
            , CASE
              WHEN attnotnull THEN
                cast('NOT NULL' as text)
              ELSE
                cast('' as text)
              END as column_null
            , CASE
              WHEN pg_type.typname IN ('int4', 'int8')
                   AND EXISTS (SELECT TRUE
                                 FROM pg_catalog.pg_depend
                                 JOIN pg_catalog.pg_class ON (pg_class.oid = objid)
                                WHERE refobjsubid = attnum
                                  AND refobjid = attrelid
                                  AND relkind = 'S') THEN
                NULL
              ELSE
                pg_get_expr(adbin, adrelid)
              END as column_default
            , pg_catalog.col_description(attrelid, attnum) as column_description
            , attnum
         FROM pg_catalog.pg_attribute 
         JOIN pg_catalog.pg_type ON (pg_type.oid = atttypid) 
    LEFT JOIN pg_catalog.pg_attrdef ON (   attrelid = adrelid 
                                       AND attnum = adnum)
        WHERE attnum > 0
          AND attisdropped IS FALSE
          AND attrelid = %(attrelid)s;
    '''
    cur.execute(request, {'attrelid': attrelid})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_indexes(cur, schemaname, tablename):
    request = '''
       SELECT schemaname
            , tablename
            , indexname
            , substring(    indexdef
                       FROM position('(' IN indexdef) + 1
                        FOR length(indexdef) - position('(' IN indexdef) - 1
                       ) AS indexdef
         FROM pg_catalog.pg_indexes
        WHERE substring(indexdef FROM 8 FOR 6) != 'UNIQUE'
          AND schemaname = %(schemaname)s
          AND tablename = %(tablename)s;
    '''
    cur.execute(request, {'schemaname': schemaname, 'tablename': tablename})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_inheritance(cur, child_schemaname, child_tablename, schemas):
    request = '''
           SELECT parnsp.nspname AS par_schemaname
            , parcla.relname AS par_tablename
            , chlnsp.nspname AS chl_schemaname
            , chlcla.relname AS chl_tablename
         FROM pg_catalog.pg_inherits
         JOIN pg_catalog.pg_class AS chlcla ON (chlcla.oid = inhrelid)
         JOIN pg_catalog.pg_namespace AS chlnsp ON (chlnsp.oid = chlcla.relnamespace)
         JOIN pg_catalog.pg_class AS parcla ON (parcla.oid = inhparent)
         JOIN pg_catalog.pg_namespace AS parnsp ON (parnsp.oid = parcla.relnamespace)
        WHERE chlnsp.nspname = %(child_schemaname)s
          AND chlcla.relname = %(child_tablename)s
          AND parnsp.nspname IN ({});
    '''.format(quoted_and_comma_separated(schemas))
    cur.execute(request, {'child_schemaname': child_schemaname, 'child_tablename': child_tablename})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_primary_keys(cur, conrelid):
    request = '''
       SELECT conname AS constraint_name
            , pg_catalog.pg_get_indexdef(d.objid) AS constraint_definition
            , CASE
              WHEN contype = 'p' THEN
                'PRIMARY KEY'
              ELSE
                'UNIQUE'
              END as constraint_type
         FROM pg_catalog.pg_constraint AS c
         JOIN pg_catalog.pg_depend AS d ON (d.refobjid = c.oid)
        WHERE contype IN ('p', 'u')
          AND deptype = 'i'
          AND conrelid = %(conrelid)s;
    '''
    cur.execute(request, {'conrelid': conrelid})
    rows = fetchall_as_list_of_dict(cur)
    return rows


    # Don't return the constraint name if it was automatically generated by
    # PostgreSQL.  The $N (where N is an integer) is not a descriptive enough
    # piece of information to be worth while including in the various outputs.
def get_foreign_keys(cur, conrelid, schemas):
    request = '''
       SELECT pg_constraint.oid
            , pg_namespace.nspname AS namespace
            , CASE WHEN substring(pg_constraint.conname FROM 1 FOR 1) = '\\$' THEN ''
              ELSE pg_constraint.conname
              END AS constraint_name
            , conkey AS constraint_key
            , confkey AS constraint_fkey
            , confrelid AS foreignrelid
         FROM pg_catalog.pg_constraint
         JOIN pg_catalog.pg_class ON (pg_class.oid = conrelid)
         JOIN pg_catalog.pg_class AS pc ON (pc.oid = confrelid)
         JOIN pg_catalog.pg_namespace ON (pg_class.relnamespace = pg_namespace.oid)
         JOIN pg_catalog.pg_namespace AS pn ON (pn.oid = pc.relnamespace)
        WHERE contype = 'f'
          AND conrelid = %(conrelid)s
          AND pg_namespace.nspname IN ({})
          AND pn.nspname IN ({});
    '''.format(quoted_and_comma_separated(schemas), quoted_and_comma_separated(schemas))
    cur.execute(request, {'conrelid': conrelid})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_foreign_key_arg(cur, attrelid, attnum):
    request = '''
       SELECT attname AS attribute_name
            , relname AS relation_name
            , nspname AS namespace
         FROM pg_catalog.pg_attribute
         JOIN pg_catalog.pg_class ON (pg_class.oid = attrelid)
         JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
        WHERE attrelid = %(attrelid)s
          AND attnum = %(attnum)s;
    '''
    cur.execute(request, {'attrelid': attrelid, 'attnum': attnum})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_constraint(cur, conrelid):
    request = '''
       SELECT pg_get_constraintdef(oid) AS constraint_source
            , conname AS constraint_name
         FROM pg_constraint
        WHERE conrelid = %(conrelid)s
          AND contype = 'c';
    '''
    cur.execute(request, {'conrelid': conrelid})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_function_arg(cur, type_oid):
    request = '''
       SELECT nspname AS namespace
            , replace( pg_catalog.format_type(pg_type.oid, typtypmod)
                     , nspname ||'.'
                     , '') AS type_name
         FROM pg_catalog.pg_type
         JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = typnamespace)
        WHERE pg_type.oid = %(type_oid)s;
    '''
    cur.execute(request, {'type_oid': type_oid})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_schemas_comment(cur, schemas):
    request = '''
       SELECT pg_catalog.obj_description(oid, 'pg_namespace') AS comment
            , nspname as namespace
         FROM pg_catalog.pg_namespace
        WHERE pg_namespace.nspname IN ({});
    '''.format(quoted_and_comma_separated(schemas))
    cur.execute(request)
    rows = fetchall_as_list_of_dict(cur)
    return rows


def get_functions(cur, schema, functions_whitelist_regex, functions_blacklist_regex):
    functions_whitelist_regex = regex_from_json(functions_whitelist_regex, '^')
    functions_blacklist_regex = regex_from_json(functions_blacklist_regex, '^$')
    request = '''
       SELECT proname AS function_name
            , nspname AS namespace
            , lanname AS language_name
            , pg_catalog.obj_description(pg_proc.oid, 'pg_proc') AS comment
            , proargtypes AS function_args
            , proargnames AS function_arg_names
            , prosrc AS source_code
            , proretset AS returns_set
            , prorettype AS return_type
         FROM pg_catalog.pg_proc
         JOIN pg_catalog.pg_language ON (pg_language.oid = prolang)
         JOIN pg_catalog.pg_namespace ON (pronamespace = pg_namespace.oid)
         JOIN pg_catalog.pg_type ON (prorettype = pg_type.oid)
        WHERE pg_namespace.nspname = %(schema)s
          AND proname ~ %(functions_whitelist_regex)s
          AND proname !~ %(functions_blacklist_regex)s
    '''
    cur.execute(request, {'schema': schema,
                          'functions_whitelist_regex': functions_whitelist_regex,
                          'functions_blacklist_regex': functions_blacklist_regex})
    rows = fetchall_as_list_of_dict(cur)
    return rows


def main():
    # Database Connection
    conn = psycopg2.connect(database='sandbox', user='postgres', password=1, host='localhost', port=5432)
    conn.set_client_encoding('UTF8')
    cur = conn.cursor()

    result = dict()

    result['database_description'] = get_database_description(cur, 'sandbox')

    schemas = result['schemas'] = get_schemas(
        cur, None, '^(pg_catalog|pg_toast|pg_toast_temp_[0-9]+|pg_temp_[0-9]+|information_schema)$')

    result['tables'] = list()
    for schema in result['schemas']:
        result['tables'] += get_tables(cur, schema, None, None)

    for table in result['tables']:
        reloid = table['oid']
        relname = table['tablename']
        schema = table['namespace']
        if table['reltype'] == 'table':
            table['statistics'] = get_statistics(cur, reloid)
        table['columns'] = get_columns(cur, reloid)
        table['indexes'] = get_indexes(cur, schema, relname)
        table['inheritance'] = get_inheritance(cur, schema, relname, schemas)
        table['primary_keys'] = get_primary_keys(cur, reloid)
        foreign_keys = table['foreign_keys'] = get_foreign_keys(cur, reloid, schemas)
        for forcols in foreign_keys:
            fkeyset = forcols['constraint_fkey']
            keyset = forcols['constraint_key']
            frelid = forcols['foreignrelid']

            forcols['constraint_fkey_arg'] = list()
            for k in fkeyset:
                forcols['constraint_fkey_arg'].append(get_foreign_key_arg(cur, frelid, k))

            forcols['constraint_key_arg'] = list()
            for k in keyset:
                forcols['constraint_key_arg'].append(get_foreign_key_arg(cur, reloid, k))

        table['constraints'] = get_constraint(cur, reloid)

    result['functions'] = list()
    for schema in result['schemas']:
        result['functions'] += get_functions(cur, schema, None, None)

    for function in result['functions']:
        functionargs = function['function_args']
        types = functionargs.split()
        function['function_args_info'] = list()
        for type_oid in types:
            function['function_args_info'].append(get_function_arg(cur, type_oid))
        function['return_type_info'] = get_function_arg(cur, function['return_type'])

    result['schemas_comment'] = get_schemas_comment(cur, schemas)

    print(json.dumps(result, indent=2, cls=PgJsonEncoder))

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
