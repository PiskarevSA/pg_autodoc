# command line arguments to run in sandbox:
#   -d sandbox -u postgres --password 1 -t html
import argparse
from decimal import Decimal
import json
import os
import psycopg2
import re
import sys


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


def set_schema_comment(struct, schema, comment):
    struct. \
        setdefault(schema, dict()). \
        setdefault('SCHEMA', dict())['COMMENT'] = comment


def set_table_attribute(struct, schema, table, name, value):
    struct. \
        setdefault(schema, dict()). \
        setdefault('TABLE', dict()). \
        setdefault(table, dict())[name] = value


def set_column_attribute(struct, schema, table, column, name, value):
    struct. \
        setdefault(schema, dict()). \
        setdefault('TABLE', dict()). \
        setdefault(table, dict()). \
        setdefault('COLUMN', dict()). \
        setdefault(column, dict())[name] = value


def set_column_constraint_attribute(struct, schema, table, column, constraint, name, value):
    struct. \
        setdefault(schema, dict()). \
        setdefault('TABLE', dict()). \
        setdefault(table, dict()). \
        setdefault('COLUMN', dict()). \
        setdefault(column, dict()). \
        setdefault('CON', dict()). \
        setdefault(constraint, dict())[name] = value


def set_index_definition(struct, schema, table, index_name, index_definition):
    struct. \
        setdefault(schema, dict()). \
        setdefault('TABLE', dict()). \
        setdefault(table, dict()). \
        setdefault('INDEX', dict())[index_name] = index_definition


def set_permission_granted(struct, schema, table, user, permission):
    struct. \
        setdefault(schema, dict()). \
        setdefault('TABLE', dict()). \
        setdefault(table, dict()). \
        setdefault('ACL', dict()). \
        setdefault(user, dict())[permission] = 1


def set_constraint(struct, schema, table, constraint_name, constraint_source):
    struct. \
        setdefault(schema, dict()). \
        setdefault('TABLE', dict()). \
        setdefault(table, dict()). \
        setdefault('CONSTRAINT', dict())[constraint_name] = constraint_source


def set_table_inherit(struct, schema, table, parent_schemaname, parent_tablename):
    struct. \
        setdefault(schema, dict()). \
        setdefault('TABLE', dict()). \
        setdefault(table, dict()). \
        setdefault('INHERIT', dict()). \
        setdefault(parent_schemaname, dict())[parent_tablename] = 1


def set_function_attribute(struct, schema, function, name, value):
    struct. \
        setdefault(schema, dict()). \
        setdefault('FUNCTION', dict()). \
        setdefault(function, dict())[name] = value


class PgJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(PgJsonEncoder, self).default(o)


def main():
    argv = sys.argv
    db = dict()

    # The templates path
    template_path = '/usr/local/share/postgresql_autodoc'

    # Setup the default connection variables based on the environment
    dbuser = os.getenv('PGUSER') or os.getenv('USER')
    database = os.getenv('PGDATABASE') or os.getenv('USER')
    dbhost = os.getenv('PGHOST')
    dbport = os.getenv('PGPORT')

    # Determine whether we need a password to connect
    needpass = 0
    dbpass = None
    output_filename_base = database

    # Tracking variables
    dbisset = 0
    fileisset = 0

    only_schema = None
    only_matching = None
    table_out = None
    wanted_output = None  # means all types
    statistics = 0

    # Fetch base and dirnames.  Useful for Usage()
    dirname, basename = os.path.split(argv[0])

    # If template_path isn't defined, lets set it ourselves
    if template_path is None:
        template_path = dirname

    parser = argparse.ArgumentParser(
        description='This is a utility which will run through PostgreSQL system tables and returns HTML, DOT, '
                    'and several styles of XML which describe the database. As a result, documentation about a '
                    'project can be generated quickly and be automatically updatable, yet have a quite professional '
                    'look if you do some DSSSL/CSS work.')
    parser.add_argument('-d', metavar='<dbname>', type=str,
                        help='Specify database name to connect to (default: {})'.format(database))
    parser.add_argument('-f', metavar='<file>', type=str,
                        help='Specify output file prefix (default: {})'.format(database))
    parser.add_argument('--host', metavar='<host>', type=str,
                        help='Specify database server host (default: localhost)')
    parser.add_argument('-p', metavar='<port>', type=int,
                        help='Specify database server port (default: 5432)')
    parser.add_argument('-u', metavar='<username>', type=str,
                        help='Specify database username (default: {})'.format(dbuser))
    parser.add_argument('--password', metavar='<pw>', type=str,
                        help='Specify database password (default: blank)')
    parser.add_argument('--prompt-password', action="store_true",
                        help='Have {} prompt for a password'.format(basename))
    parser.add_argument('-l', '--library', metavar='<path>', type=str,
                        help='Path to the templates (default: {})'.format(template_path))
    parser.add_argument('-t', '--type', metavar='<output>', type=str,
                        help='Type of output wanted (default: All in template library)')
    parser.add_argument('-s', '--schema', metavar='<schema>', type=str,
                        help='Specify a specific schema to match. Technically this is a regular expression but '
                             'anything other than a specific name may have unusual results')
    parser.add_argument('-m', '--matching', metavar='<regexp>', type=str,
                        help='Show only tables/objects with names matching the specified regular expression')
    parser.add_argument('-w', action="store_true",
                        help='Use ~/.pgpass for authentication (overrides all other password options)')
    parser.add_argument('--table', metavar='<args>', type=str,
                        help='Tables to export. Multiple tables may be provided using a comma-separated list. I.e. '
                             'table,table2,table3')
    parser.add_argument('--statistics', action="store_true",
                        help='In 7.4 and later, with the contrib module pgstattuple installed we can gather '
                             'statistics on the tables in the database (average size, free space, disk space used, '
                             'dead tuple counts, etc.) This is disk intensive on large databases as all pages must be '
                             'visited')
    args = parser.parse_args()

    # Set the database
    if args.d is not None:
        database = args.d
        dbisset = 1
        if not fileisset:
            output_filename_base = database

    # Set the user
    if args.u is not None:
        dbuser = args.u
        if not dbisset:
            database = dbuser
            if not fileisset:
                output_filename_base = database

    # Set the hostname
    if args.host is not None:
        dbhost = args.host

    # Set the Port
    if args.p is not None:
        dbport = args.p

    # Set the users password
    if args.password is not None:
        dbpass = args.password

    # Make sure we get a password before attempting to conenct
    if args.prompt_password:
        needpass = 1

    # Read from .pgpass (override all other password options)
    if args.w:
        dbpass = None
        dbuser = None
        needpass = 0

    # Set the base of the filename. The extensions pulled
    # from the templates will be appended to this name
    if args.f is not None:
        output_filename_base = args.f
        fileisset = 1

    # Set the template directory explicitly
    if args.library is not None:
        template_path = args.library

    # Set the output type
    if args.type is not None:
        wanted_output = args.type

    # User has requested a single schema dump and provided a pattern
    if args.schema is not None:
        only_schema = args.schema

    # User has requested only tables/objects matching a pattern
    if args.matching is not None:
        only_matching = args.matching

    # One might dump a table's set (comma-separated) or just one
    # If dumping a set of specific tables do NOT dump out the functions
    # in this database. Generates noise in the output
    # that most likely isn't wanted. Check for $table_out around the
    # function gathering location.
    if args.table is not None:
        tables_in = args.table.split(',')
        table_out = ','.join(["'{}'".format(table) for table in tables_in])

    # Check to see if Statistics have been requested
    if args.statistics:
        statistics = 1

    # If no arguments have been provided, connect to the database anyway but
    # inform the user of what we're doing.
    if len(sys.argv) == 1:
        print("No arguments set.  Use '{} --help' for help\n"
              "\n"
              "Connecting to database '{}' as user '{}'".format(basename, database, dbuser))

    # If needpass has been set but no password was provided, prompt the user
    # for a password.
    if needpass and dbpass is None:
        dbpass = input("Password: ")

    # Database Connection
    dbhost = dbhost if dbhost is not None else 'localhost'
    dbport = dbport if dbport is not None else 5432
    conn = psycopg2.connect(database=database, user=dbuser, password=dbpass, host=dbhost, port=dbport)
    conn.set_client_encoding('UTF8')

    info_collect(conn, db, database, only_schema, only_matching, statistics, table_out)
    conn.close()

    # Write out *ALL* templates
    write_using_templates(db, database, statistics, template_path, output_filename_base, wanted_output)


##
# info_collect
#
# Pull out all of the applicable information about a specific database
def info_collect(conn, db, database, only_schema, only_matching, statistics, table_out):
    struct = db['STRUCT'] = dict()

    # PostgreSQL's version is used to determine what queries are required
    # to retrieve a given information set.
    if conn.server_version < 70300:
        raise RuntimeError("PostgreSQL 7.3 and later are supported")

    # Ensure we only retrieve information for the requested schemas.
    #
    # system_schema         -> The primary system schema for a database.
    #                       Public is used for versions prior to 7.3
    #
    # system_schema_list -> The list of schemas which we are not supposed
    #                       to gather information for.
    #                        TODO: Merge with system_schema in array form.
    #
    # schemapattern      -> The schema the user provided as a command
    #                       line option.
    system_schema = 'pg_catalog'
    system_schema_list = 'pg_catalog|pg_toast|pg_temp_[0-9]+|information_schema'
    schemapattern = '^' if only_schema is None else '^' + only_schema + '$'

    # and only objects matching the specified pattern, if any
    matchpattern = '' if only_matching is None else only_matching

    #
    # List of queries which are used to gather information from the
    # database. The queries differ based on version but should
    # provide similar output. At some point it should be safe to remove
    # support for older database versions.
    #

    # Fetch the description of the database
    sql_database = '''
       SELECT pg_catalog.shobj_description(oid, 'pg_database') as comment
         FROM pg_catalog.pg_database
        WHERE datname = '{}';
    '''.format(database)

    # Pull out a list of tables, views and special structures.
    sql_tables = '''
       SELECT nspname as namespace
            , relname as tablename
            , pg_catalog.pg_get_userbyid(relowner) AS tableowner
            , pg_class.oid
            , pg_catalog.obj_description(pg_class.oid, 'pg_class') as table_description
            , relacl
            , CASE
              WHEN relkind = 'r' THEN
                'table'
              WHEN relkind = 's' THEN
                'special'
              ELSE
                'view'
              END as reltype
            , CASE
              WHEN relkind = 'v' THEN
                pg_get_viewdef(pg_class.oid)
              ELSE
                NULL
              END as view_definition
         FROM pg_catalog.pg_class
         JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
        WHERE relkind IN ('r', 's', 'v')
          AND relname ~ '{}'
          AND nspname !~ '{}'
          AND nspname ~ '{}' 
    '''.format(matchpattern, system_schema_list, schemapattern)
    if table_out is not None:
        sql_tables = sql_tables + 'AND relname IN {}'.format(table_out)

    # - uses pg_class.oid
    sql_columns = '''
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

    sql_table_statistics = None
    if statistics == 1:
        if conn.server_version < 70400:
            raise RuntimeError("Table statistics supported on PostgreSQL 7.4 and later.\n"
                               "Remove --statistics flag and try again.")
        sql_table_statistics = '''
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

    sql_indexes = '''
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

    sql_inheritance = '''
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
          AND chlnsp.nspname ~ '{}'
          AND parnsp.nspname ~ '{}';
    '''.format(schemapattern, schemapattern)

    # Fetch the list of PRIMARY and UNIQUE keys
    sql_primary_keys = '''
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

    # FOREIGN KEY fetch
    #
    # Don't return the constraint name if it was automatically generated by
    # PostgreSQL.  The $N (where N is an integer) is not a descriptive enough
    # piece of information to be worth while including in the various outputs.
    sql_foreign_keys = '''
       SELECT pg_constraint.oid
            , pg_namespace.nspname AS namespace
            , CASE WHEN substring(pg_constraint.conname FROM 1 FOR 1) = '\$' THEN ''
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
          AND pg_namespace.nspname ~ '{}'
          AND pn.nspname ~ '{}';
    '''.format(schemapattern, schemapattern)

    sql_foreign_key_arg = '''
       SELECT attname AS attribute_name
            , relname AS relation_name
            , nspname AS namespace
         FROM pg_catalog.pg_attribute
         JOIN pg_catalog.pg_class ON (pg_class.oid = attrelid)
         JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
        WHERE attrelid = %(attrelid)s
          AND attnum = %(attnum)s;
    '''

    # Fetch CHECK constraints
    sql_constraint = '''
       SELECT pg_get_constraintdef(oid) AS constraint_source
            , conname AS constraint_name
         FROM pg_constraint
        WHERE conrelid = %(conrelid)s
          AND contype = 'c';
    '''

    # Query for function information
    sql_functions = '''
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
        WHERE pg_namespace.nspname !~ '{}'
          AND pg_namespace.nspname ~ '{}'
          AND proname ~ '{}'
          AND proname != 'plpgsql_call_handler';
    '''.format(system_schema_list, schemapattern, matchpattern)

    sql_function_arg = '''
       SELECT nspname AS namespace
            , replace( pg_catalog.format_type(pg_type.oid, typtypmod)
                     , nspname ||'.'
                     , '') AS type_name
         FROM pg_catalog.pg_type
         JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = typnamespace)
        WHERE pg_type.oid = %(type_oid)s;
    '''

    sql_schemas = '''
       SELECT pg_catalog.obj_description(oid, 'pg_namespace') AS comment
            , nspname as namespace
         FROM pg_catalog.pg_namespace
        WHERE pg_namespace.nspname !~ '{}'
          AND pg_namespace.nspname ~ '{}';
    '''.format(system_schema_list, schemapattern)

    cur = conn.cursor()

    # Fetch Database info
    cur.execute(sql_database)
    rows = fetchall_as_list_of_dict(cur)
    if rows:
        db['COMMENT'] = rows[0]['comment']

    # Fetch tables and all things bound to tables
    permission_flag_to_str = {
        'a': 'INSERT',
        'r': 'SELECT',
        'w': 'UPDATE',
        'd': 'DELETE',
        'R': 'RULE',
        'x': 'REFERENCES',
        't': 'TRIGGER',
    }
    cur.execute(sql_tables)
    tables = fetchall_as_list_of_dict(cur)
    for table in tables:
        reloid = table['oid']
        relname = table['tablename']
        schema = table['namespace']

        # Store permissions
        acl = table['relacl']

        # Empty acl groups cause serious issues.
        acl = '' if acl is None else acl

        # Strip array forming 'junk'.
        acl = acl.strip('{}').replace('"', '')

        # Foreach acl
        for acl_item in acl.split(','):
            if not acl_item:
                continue
            user, raw_permissions = acl_item.split('=')
            if raw_permissions:
                user = 'PUBLIC' if not user else user

            # The section after the / is the user who granted the permissions
            permissions, granting_user = raw_permissions.split('/')

            # Break down permissions to individual flags
            for flag in permissions:
                permission = permission_flag_to_str.setdefault(flag, 'FLAG_{}'.format(flag))  # fall back if unexpected
                set_permission_granted(struct, schema, relname, user, permission)

        # Primitive Stats, but only if requested
        if statistics == 1 and table['reltype'] == 'table':
            cur.execute(sql_table_statistics, {'table_oid': reloid, })
            stats = fetchall_as_list_of_dict(cur)
            assert len(stats) == 1
            set_table_attribute(struct, schema, relname, 'TABLELEN', stats[0]['table_len'])
            set_table_attribute(struct, schema, relname, 'TUPLECOUNT', stats[0]['tuple_count'])
            set_table_attribute(struct, schema, relname, 'TUPLELEN', stats[0]['tuple_len'])
            set_table_attribute(struct, schema, relname, 'DEADTUPLELEN', stats[0]['dead_tuple_len'])
            set_table_attribute(struct, schema, relname, 'FREELEN', stats[0]['free_space'])

        # Store the relation type
        set_table_attribute(struct, schema, relname, 'TYPE', table['reltype'])

        # Store table description
        set_table_attribute(struct, schema, relname, 'DESCRIPTION', table['table_description'])

        # Store the view definition
        set_table_attribute(struct, schema, relname, 'VIEW_DEF', table['view_definition'])

        # Store constraints
        cur.execute(sql_constraint, {'conrelid': reloid, })
        constraints = fetchall_as_list_of_dict(cur)
        for constraint in constraints:
            constraint_name = constraint['constraint_name']
            constraint_source = constraint['constraint_source']
            set_constraint(struct, schema, relname, constraint_name, constraint_source)

        cur.execute(sql_columns, {'attrelid': reloid, })
        columns = fetchall_as_list_of_dict(cur)
        for column in columns:
            column_name = column['column_name']
            set_column_attribute(struct, schema, relname, column_name, 'ORDER', column['attnum'])
            set_column_attribute(struct, schema, relname, column_name, 'PRIMARY KEY', 0)
            set_column_attribute(struct, schema, relname, column_name, 'FKTABLE', '')
            set_column_attribute(struct, schema, relname, column_name, 'TYPE', column['column_type'])
            set_column_attribute(struct, schema, relname, column_name, 'NULL', column['column_null'])
            set_column_attribute(struct, schema, relname, column_name, 'DESCRIPTION', column['column_description'])
            set_column_attribute(struct, schema, relname, column_name, 'DEFAULT', column['column_default'])

        # Pull out both PRIMARY and UNIQUE keys based on the supplied query
        # and the relation OID.
        #
        # Since there may be multiple UNIQUE indexes on a table, we append a
        # number to the end of the the UNIQUE keyword which shows that they
        # are a part of a related definition.  I.e UNIQUE_1 goes with UNIQUE_1
        #
        cur.execute(sql_primary_keys, {'conrelid': reloid, })
        primary_keys = fetchall_as_list_of_dict(cur)
        unqgroup = 0
        for pricols in primary_keys:
            index_type = pricols['constraint_type']
            con = pricols['constraint_name']
            indexdef = pricols['constraint_definition']

            # Fetch the column list
            column_list = indexdef
            column_list = re.sub(".*\\(([^)]+)\\).*", "\\1", column_list)

            # Split our column list and deal with all PRIMARY KEY fields
            collist = column_list.split(',')

            # Store the column number in the indextype field.  Anything > 0
            # indicates the column has this type of constraint applied to it.
            numcols = len(collist)

            # Bump group number if there are two or more columns
            if numcols >= 2 and index_type == 'UNIQUE':
                unqgroup = unqgroup + 1

            # Record the data to the structure.
            for column_index, column in enumerate(collist):
                column = column.strip().strip('"')
                set_column_constraint_attribute(struct, schema, relname, column, con, 'TYPE', index_type)
                set_column_constraint_attribute(struct, schema, relname, column, con, 'COLNUM', column_index + 1)

                # Record group number only when a multi-column
                # constraint is involved
                if numcols >= 2 and index_type == 'UNIQUE':
                    set_column_constraint_attribute(struct, schema, relname, column, con, 'KEYGROUP', unqgroup)

        # FOREIGN KEYS like UNIQUE indexes can appear several times in
        # a table in multi-column format. We use the same trick to
        # record a numeric association to the foreign key reference.
        cur.execute(sql_foreign_keys, {'conrelid': reloid, })
        foreign_keys = fetchall_as_list_of_dict(cur)
        fkgroup = 0
        for forcols in foreign_keys:
            column_oid = forcols['oid']
            con = forcols['constraint_name']

            # Declare variables for dataload
            keylist = list()
            fkeylist = list()
            fschema = None
            ftable = None

            fkey = forcols['constraint_fkey']
            keys = forcols['constraint_key']
            frelid = forcols['foreignrelid']

            # Since decent array support was not added until 7.4, and
            # we want to support 7.3 as well, we parse the text version
            # of the array by hand rather than combining this and
            # Foreign_Key_Arg query into a single query.

            fkeyset = list()
            if isinstance(fkey, list):
                fkeyset = fkey
            else:  # DEPRECATED: DBD::Pg 1.49 and earlier
                fkeyset = fkey.strip('{}').replace('"', '').split(',')

            keyset = list()
            if isinstance(keys, list):
                keyset = keys
            else:  # DEPRECATED: DBD::Pg 1.49 and earlier
                keyset = keys.strip('{}').replace('"', '').split(',')

            # Convert the list of column numbers into column names for the
            # local side.
            for k in keyset:
                cur.execute(sql_foreign_key_arg, {'attrelid': reloid, 'attnum': k})
                foreign_key_arg = fetchall_as_list_of_dict(cur)
                assert len(foreign_key_arg) == 1
                keylist.append(foreign_key_arg[0]['attribute_name'])

            # Convert the list of columns numbers into column names
            # for the referenced side. Grab the table and namespace
            # while we're here.
            for k in fkeyset:
                cur.execute(sql_foreign_key_arg, {'attrelid': frelid, 'attnum': k})
                foreign_key_arg = fetchall_as_list_of_dict(cur)
                assert len(foreign_key_arg) == 1
                fkeylist.append(foreign_key_arg[0]['attribute_name'])
                fschema = foreign_key_arg[0]['namespace']
                ftable = foreign_key_arg[0]['relation_name']

            # Deal with common catalog issues.
            if len(keylist) != len(fkeylist):
                raise RuntimeError('FKEY {} Broken -- fix your PostgreSQL installation'.format(con))

            # Load up the array based on the information discovered
            # using the information retrieval methods above.
            numcols = len(keylist)

            # Bump group number if there are two or more columns involved
            if numcols >= 2 :
                fkgroup = fkgroup + 1

            # Record the foreign key to structure
            for column_index, column, fkey in zip(range(numcols), keylist, fkeylist):
                set_column_constraint_attribute(struct, schema, relname, column, con, 'TYPE', 'FOREIGN KEY')
                set_column_constraint_attribute(struct, schema, relname, column, con, 'COLNUM', column_index + 1)
                set_column_constraint_attribute(struct, schema, relname, column, con, 'FKTABLE', ftable)
                set_column_constraint_attribute(struct, schema, relname, column, con, 'FKSCHEMA', fschema)
                set_column_constraint_attribute(struct, schema, relname, column, con, 'FK-COL NAME', fkey)

                # Record group number only when a multi-column
                # constraint is involved
                if numcols >= 2:
                    set_column_constraint_attribute(struct, schema, relname, column, con, 'KEYGROUP', fkgroup)

        # Pull out index information
        cur.execute(sql_indexes, {'schemaname': schema, 'tablename': relname})
        indexes = fetchall_as_list_of_dict(cur)
        for idx in indexes:
            index_name = idx['indexname']
            index_definition = idx['indexdef']
            set_index_definition(struct, schema, relname, index_name, index_definition)

        # Extract Inheritance information
        cur.execute(sql_inheritance, {'child_schemaname': schema, 'child_tablename': relname})
        inheritance = fetchall_as_list_of_dict(cur)
        for inherit in inheritance:
            parent_schemaname = inherit['par_schemaname']
            parent_tablename = inherit['par_tablename']
            set_table_inherit(struct, schema, relname, parent_schemaname, parent_tablename)

    # Function Handling
    if table_out is None:
        cur.execute(sql_functions)
        functions = fetchall_as_list_of_dict(cur)
        for function in functions:
            schema = function['namespace']
            comment = function['comment']
            functionargs = function['function_args']
            types = functionargs.split()
            count = 0

            # Pre-setup argument names when available.
            argnames = function['function_arg_names']

            # Setup full argument types including the parameter name
            parameters = list()
            for type_oid in types:
                cur.execute(sql_function_arg, {'type_oid': type_oid, })
                function_arg = fetchall_as_list_of_dict(cur)
                assert len(function_arg) == 1
                parameter = argnames.pop(0) + ' ' if argnames else ''
                if function_arg[0]['namespace'] != system_schema:
                    parameter = parameter + function_arg[0]['namespace'] + '.'
                parameter = parameter + function_arg[0]['type_name']
                parameters.append(parameter)
            functionname = '{}({})'.format(function['function_name'], ', '.join(parameters))

            ret_type = 'SET OF ' if function['returns_set'] else ''
            cur.execute(sql_function_arg, {'type_oid': function['return_type']})
            function_arg = fetchall_as_list_of_dict(cur)
            assert len(function_arg) == 1
            ret_type = ret_type + function_arg[0]['type_name']

            set_function_attribute(struct, schema, functionname, 'COMMENT', comment)
            set_function_attribute(struct, schema, functionname, 'SOURCE', function['source_code'])
            set_function_attribute(struct, schema, functionname, 'LANGUAGE', function['language_name'])
            set_function_attribute(struct, schema, functionname, 'RETURNS', ret_type)

    # Deal with the Schema
    cur.execute(sql_schemas)
    schemas = fetchall_as_list_of_dict(cur)
    for schema in schemas:
        comment = schema['comment']
        namespace = schema['namespace']
        set_schema_comment(struct, namespace, comment)

    cur.close()


#####
# write_using_templates
#
# Generate structure that HTML::Template requires out of the
# $struct for table related information, and $struct for
# the schema and function information
def write_using_templates(db, database, statistics, template_path, output_filename_base, wanted_output):
    print('write_using_templates')
    as_json = json.dumps({'db': db, 'database': database, 'statistics': statistics,
                          'template_path': template_path, 'output_filename_base': output_filename_base,
                          'wanted_output': wanted_output}, indent=2, cls=PgJsonEncoder)
    print(as_json)
    pass


if __name__ == '__main__':
    main()
