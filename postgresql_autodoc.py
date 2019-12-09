# command line arguments to run in sandbox:
#   -d sandbox -u postgres --password 1 -t html
#   --host 192.168.12.207 -p 5432 -d radar_db -u postgres --statistics --password=123 --library templates
import argparse
from datetime import datetime
from decimal import Decimal
from htmltmpl import TemplateManager, TemplateProcessor
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
    print('collecting data')
    db[database] = dict()
    struct = db[database]['STRUCT'] = dict()

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
              WHEN relkind = 'm' THEN
                'materialized view'
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
        WHERE relkind IN ('r', 's', 'm', 'v')
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
        db[database]['COMMENT'] = rows[0]['comment']

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
    print('item count: {}'.format(len(tables)))
    item_index = 0
    for table in tables:
        reloid = table['oid']
        relname = table['tablename']
        schema = table['namespace']

        # Store permissions
        acl = table['relacl']

        # Empty acl groups cause serious issues.
        acl = '' if acl is None else acl

        # TODO remove this stub
        # acl = '{mydbname=arwdxt/mydbname,mydbuser=r/mydbname}'

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
            if numcols >= 2:
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

        print(
            'item {} of {} processed: {} {}.{}'.format(item_index + 1, len(tables), table['reltype'], schema, relname))
        item_index = item_index + 1

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


######
# sgml_safe_id
#   Safe SGML ID Character replacement
def sgml_safe_id(string):
    # Lets use the keyword ARRAY in place of the square brackets
    # to prevent duplicating a non-array equivelent
    string = re.sub('\\[\\]', 'ARRAY-', string)

    # Brackets, spaces, commads, underscores are not valid 'id' characters
    # replace with as few -'s as possible.
    string = re.sub('[ "\',)(_-]+', '-', string)

    # Don't want a - at the end either.  It looks silly.
    string = re.sub('-$', '', string)

    return string


#####
# useUnits
#    Tack on base 2 metric units
def use_units(value):
    if value is None:
        return ''

    units = ('Bytes', 'KiBytes', 'MiBytes', 'GiBytes', 'TiBytes')
    loop = 0

    while value >= 1024:
        loop = loop + 1
        value = value / 1024

    return '%.2f %s' % (value, units[loop])


#####
# html
#    HTML output is special in that we want to escape
#    the characters inside the string and replace line feed with <br>
def html(string):
    if string is None:
        return ''
    elif isinstance(string, int):
        return str(string)
    elif isinstance(string, str):
        string = re.sub('&(?!(amp|lt|gr|apos|quot);)', '&amp', string)
        string = re.sub('<', '&lt;', string)
        string = re.sub('>', '&gt;', string)
        string = re.sub("'", '&apos;', string)
        string = re.sub('"', '&quot;', string)
        string = re.sub('\n', '<br>', string)
    else:
        assert False
    return string


#####
# docbook
#    Docbook output is special in that we may or may not want to escape
#    the characters inside the string depending on a string prefix.
def docbook(string):
    if string is None:
        return ''
    elif isinstance(string, int):
        return str(string)
    elif isinstance(string, str):
        if re.match('^@DOCBOOK', string):
            string = re.sub('^@DOCBOOK', '', string)
        else:
            string = re.sub('&(?!(amp|lt|gr|apos|quot);)', '&amp', string)
            string = re.sub('<', '&lt;', string)
            string = re.sub('>', '&gt;', string)
            string = re.sub("'", '&apos;', string)
            string = re.sub('"', '&quot;', string)
    else:
        assert False
    return string


#####
# graphviz
#    GraphViz output requires that special characters (like " and whitespace) must be preceeded
#    by a \ when a part of a lable.
def graphviz(string):
    # Ensure we don't return an least a empty string
    if string is None:
        string = ''

    string = re.sub('([\\s"\'])', '\\\\\\1', string)

    return string


#####
# sql_prettyprint
#    Clean up SQL into something presentable
def sql_prettyprint(string):
    # If nothing has been sent in, return an empty string
    if string is None:
        return ''

    # Initialize Result string
    result = ''

    # List of tokens to split on
    tok = "SELECT|FROM|WHERE|HAVING|GROUP BY|ORDER BY|OR|AND|LEFT JOIN|RIGHT JOIN" \
          "|LEFT OUTER JOIN|LEFT INNER JOIN|INNER JOIN|RIGHT OUTER JOIN|RIGHT INNER JOIN" \
          "|JOIN|UNION ALL|UNION|EXCEPT|USING|ON|CAST|[\\(\\),]"

    key = 0
    bracket = 0
    depth = 0
    indent = 6

    # XXX: Split is wrong -- match would do
    pattern = '\\(\\"[^\\"]*\\"|\'[^\']*\'|' + tok
    elems = list()
    pos = 0
    while pos < len(string):
        m = re.search(pattern, string[pos:])
        if m is None:
            tail = string[pos:]
            elems.append(tail)
            pos = len(string)
        else:
            elem_before_token = string[pos:pos + m.start()]
            token = m.group()
            elems.append(elem_before_token)
            elems.append(token)
            pos = pos + m.start() + len(token)

    for elem in elems:
        format = None

        # Skip junk tokens
        if re.match('^[\\s]?$', elem):
            continue

        # NOTE: Should we drop leading spaces?
        #    elem.lstrip()

        # Close brackets are special
        # Bring depth in a level
        if re.match('\\)', elem):
            depth = depth - indent
            if key == 1 or bracket == 1:
                format = '%s%s'
            else:
                format = '%s\n%{}s'.format(depth)

            key = 0
            bracket = 0

        # Open brackets are special
        # Bump depth out a level
        elif re.match('\\(', elem):
            if key == 1:
                format = '%s %s'
            else:
                format = '%s\n%{}s'.format(depth)
            depth = depth + indent
            bracket = 1
            key = 0

        # Key element
        # Token from our list -- format on left hand side of the equation
        # when appropriate.
        elif re.match(tok, elem):
            if key == 1:
                format = '%s%s'
            else:
                format = '%s\n%{}s'.format(depth)

            key = 1
            bracket = 0

        # Value
        # Format for right hand side of the equation
        else:
            format = '%s%s'
            key = 0

        # Add the new format string to the result
        result = format % (result, elem)

    return result


#####
# write_using_templates
#
# Generate structure that HTML::Template requires out of the
# 'STRUCT' for table related information, and 'STRUCT' for
# the schema and function information
def write_using_templates(db, database, statistics, template_path, output_filename_base, wanted_output):
    struct = db[database]['STRUCT']

    schemas = list()

    # Start at 0, increment to 1 prior to use.
    object_id = 0
    tableids = dict()
    for schema in sorted(struct.keys()):
        schema_attr = struct[schema]
        tables = list()
        tablenames = sorted(schema_attr['TABLE'].keys()) if 'TABLE' in schema_attr else []
        for table in tablenames:
            table_attr = schema_attr['TABLE'][table]
            # Column List
            columns = list()
            columnnames = sorted(table_attr['COLUMN'].keys(),
                                 key=lambda column_name: table_attr['COLUMN'][column_name]['ORDER'])
            for column in columnnames:
                column_attr = table_attr['COLUMN'][column]
                inferrednotnull = 0

                # Have a shorter default for places that require it
                shortdefault = column_attr['DEFAULT']
                if shortdefault:
                    shortdefault = re.sub('^(.{17}).{5,}(.{5})$', '\\1 ... \\2', shortdefault)

                # Deal with column constraints
                colconstraints = list()
                connames = sorted(column_attr['CON'].keys()) if 'CON' in column_attr else []
                for con in connames:
                    con_attr = column_attr['CON'][con]
                    if con_attr['TYPE'] == 'UNIQUE':
                        unq = con_attr['TYPE']
                        unqcol = con_attr['COLNUM']
                        unqgroup = con_attr['KEYGROUP'] if 'KEYGROUP' in con_attr else None
                        colconstraints.append({
                            'column_unique': unq,
                            'column_unique_colnum': unqcol,
                            'column_unique_keygroup': unqgroup,
                        })
                    elif con_attr['TYPE'] == 'PRIMARY KEY':
                        inferrednotnull = 1
                        colconstraints.append({
                            'column_primary_key': 'PRIMARY KEY',
                        })
                    elif con_attr['TYPE'] == 'FOREIGN KEY':
                        fksgmlid = sgml_safe_id(
                            '.'.join((con_attr['FKSCHEMA'], table_attr['TYPE'], con_attr['FKTABLE'])))
                        fkgroup = con_attr['KEYGROUP'] if 'KEYGROUP' in con_attr else None
                        fktable = con_attr['FKTABLE']
                        fkcol = con_attr['FK-COL NAME']
                        fkschema = con_attr['FKSCHEMA']
                        colconstraints.append({
                            'column_fk': 'FOREIGN KEY',
                            'column_fk_colnum': fkcol,
                            'column_fk_keygroup': fkgroup,
                            'column_fk_schema': fkschema,
                            'column_fk_schema_dbk': docbook(fkschema),
                            'column_fk_schema_dot': graphviz(fkschema),
                            'column_fk_sgmlid': fksgmlid,
                            'column_fk_table': fktable,
                            'column_fk_table_dbk': docbook(fktable),
                        })

                        # only have the count if there is more than 1 schema
                        if len(struct) > 1:
                            colconstraints[-1]['number_of_schemas'] = len(struct)

                # Generate the Column array
                columns.append({
                    'column': column,
                    'column_dbk': docbook(column),
                    'column_dot': graphviz(column),
                    'column_default': column_attr['DEFAULT'],
                    'column_default_dbk': docbook(column_attr['DEFAULT']),
                    'column_default_short': shortdefault,
                    'column_default_short_dbk': docbook(shortdefault),

                    'column_comment': column_attr['DESCRIPTION'],
                    'column_comment_dbk': docbook(column_attr['DESCRIPTION']),
                    'column_comment_html': html(column_attr['DESCRIPTION']),

                    'column_number': column_attr['ORDER'],

                    'column_type': column_attr['TYPE'],
                    'column_type_dbk': docbook(column_attr['TYPE']),
                    'column_type_dot': graphviz(column_attr['TYPE']),

                    'column_constraints': colconstraints,
                })

                if inferrednotnull == 0:
                    columns[-1]["column_constraint_notnull"] = column_attr['NULL']

            # Constraint List
            constraints = list()
            for constraint in sorted(table_attr['CONSTRAINT'].keys() if 'CONSTRAINT' in table_attr else []):
                shortcon = table_attr['CONSTRAINT'][constraint]
                shortcon = re.sub('^(.{30}).{5,}(.{5})$', '\\1 ... \\2', shortcon)
                constraints.append({
                    'constraint': table_attr['CONSTRAINT'][constraint],
                    'constraint_dbk': docbook(table_attr['CONSTRAINT'][constraint]),
                    'constraint_name': constraint,
                    'constraint_name_dbk': docbook(constraint),
                    'constraint_short': shortcon,
                    'constraint_short_dbk': docbook(shortcon),
                    'table': table,
                    'table_dbk': docbook(table),
                    'table_dot': graphviz(table),
                })

            # Index List
            indexes = list()
            for index in sorted(table_attr['INDEX'].keys() if 'INDEX' in table_attr else []):
                indexes.append({
                    'index_definition': table_attr['INDEX'][index],
                    'index_definition_dbk': docbook(table_attr['INDEX'][index]),
                    'index_name': index,
                    'index_name_dbk': docbook(index),
                    'table': table,
                    'table_dbk': docbook(table),
                    'table_dot': graphviz(table),
                    'schema': schema,
                    'schema_dbk': docbook(schema),
                    'schema_dot': graphviz(schema),
                })

            inherits = list()
            for inhSch in sorted(table_attr['INHERIT'].keys() if 'INHERIT' in table_attr else []):
                for inhTab in sorted(table_attr['INHERIT'][inhSch].keys()):
                    inherits.append({
                        'table': table,
                        'table_dbk': docbook(table),
                        'table_dot': graphviz(table),
                        'schema': schema,
                        'schema_dbk': docbook(schema),
                        'schema_dot': graphviz(schema),
                        'sgmlid': sgml_safe_id('.'.join((schema, 'table', table,))),
                        'parent_sgmlid': sgml_safe_id('.'.join((inhSch, 'table', inhTab))),
                        'parent_table': inhTab,
                        'parent_table_dbk': docbook(inhTab),
                        'parent_table_dot': graphviz(inhTab),
                        'parent_schema': inhSch,
                        'parent_schema_dbk': docbook(inhSch),
                        'parent_schema_dot': graphviz(inhSch),
                    })

            # Foreign Key Discovery
            #
            # lastmatch is used to ensure that we only supply a result a
            # single time and not once for each link found.  Since the
            # loops are sorted, we only need to track the last element, and
            # not all supplied elements.
            fk_schemas = list()
            lastmatch = tuple()
            for fk_schema in sorted(struct.keys()):
                fk_schema_attr = struct[fk_schema]
                for fk_table in sorted(fk_schema_attr['TABLE'] if 'TABLE' in fk_schema_attr else []):
                    fk_table_attr = fk_schema_attr['TABLE'][fk_table]
                    for fk_column in sorted(fk_table_attr['COLUMN'] if 'COLUMN' in fk_table_attr else []):
                        fk_column_attr = fk_table_attr['COLUMN'][fk_column]
                        for fk_con in sorted(fk_column_attr['CON'] if 'CON' in fk_column_attr else []):
                            con_attr = fk_column_attr['CON'][fk_con]
                            if con_attr['TYPE'] == 'FOREIGN KEY' and con_attr['FKTABLE'] == table and con_attr[
                                'FKSCHEMA'] == schema and lastmatch != (fk_schema, fk_table):
                                fksgmlid = sgml_safe_id('.'.join((fk_schema, fk_table_attr['TYPE'], fk_table)))
                                fk_schemas.append({
                                    'fk_column_number': fk_column_attr['ORDER'],
                                    'fk_sgmlid': fksgmlid,
                                    'fk_schema': fk_schema,
                                    'fk_schema_dbk': docbook(fk_schema),
                                    'fk_schema_dot': graphviz(fk_schema),
                                    'fk_table': fk_table,
                                    'fk_table_dbk': docbook(fk_table),
                                    'fk_table_dot': graphviz(fk_table),
                                })

                                # only have the count if there is more than 1 schema
                                if len(struct) > 1:
                                    fk_schemas[-1]["number_of_schemas"] = len(struct)

                                lastmatch = (fk_schema, fk_table)

            # List off permissions
            permissions = list()
            for user in sorted(table_attr['ACL'] if 'ACL' in table_attr else []):
                permissions.append({
                    'schema': schema,
                    'schema_dbk': docbook(schema),
                    'schema_dot': graphviz(schema),
                    'table': table,
                    'table_dbk': docbook(table),
                    'table_dot': graphviz(table),
                    'user': user,
                    'user_dbk': docbook(user),
                })

                # only have the count if there is more than 1 schema
                if len(struct) > 1:
                    permissions[-1]["number_of_schemas"] = len(struct)

                for perm in table_attr['ACL'][user].keys():
                    if table_attr['ACL'][user][perm] == 1:
                        perm_lower = re.sub('^FLAG_', 'flag_', perm) if perm.startswith('FLAG_') else perm.lower()
                        permissions[-1][perm_lower] = 1

            # Increment and record the object ID
            object_id = object_id + 1
            tableids[schema + '.' + table] = object_id
            viewdef = sql_prettyprint(table_attr['VIEW_DEF'])

            # Truncate comment for Dia
            comment_dia = table_attr['DESCRIPTION']
            if comment_dia:
                comment_dia = re.sub('^(.{35}).{5,}(.{5})$', '\\1 ... \\2', comment_dia)

            table_stat_attr = lambda name: table_attr[name] if name in table_attr else None

            tables.append({
                'object_id': object_id,
                'object_id_dbk': docbook(object_id),

                'schema': schema,
                'schema_dbk': docbook(schema),
                'schema_dot': graphviz(schema),
                'schema_sgmlid': sgml_safe_id(schema + '.schema'),

                # Statistics
                'stats_enabled': statistics,
                'stats_dead_bytes': use_units(table_stat_attr('DEADTUPLELEN')),
                'stats_dead_bytes_dbk': docbook(use_units(table_stat_attr('DEADTUPLELEN'))),
                'stats_free_bytes': use_units(table_stat_attr('FREELEN')),
                'stats_free_bytes_dbk': docbook(use_units(table_stat_attr('FREELEN'))),
                'stats_table_bytes': use_units(table_stat_attr('TABLELEN')),
                'stats_table_bytes_dbk': docbook(use_units(table_stat_attr('TABLELEN'))),
                'stats_tuple_count': table_stat_attr('TUPLECOUNT'),
                'stats_tuple_count_dbk': docbook(table_stat_attr('TUPLECOUNT')),
                'stats_tuple_bytes': use_units(table_stat_attr('TUPLELEN')),
                'stats_tuple_bytes_dbk': docbook(use_units(table_stat_attr('TUPLELEN'))),

                'table': table,
                'table_dbk': docbook(table),
                'table_dot': graphviz(table),
                'table_type': table_attr['TYPE'],
                'table_type_dbk': docbook(table_attr['TYPE']),
                'table_sgmlid': sgml_safe_id('.'.join((schema, table_attr['TYPE'], table))),
                'table_comment': table_attr['DESCRIPTION'],
                'table_comment_dbk': docbook(table_attr['DESCRIPTION']),
                'table_comment_dia': comment_dia,
                'table_comment_html': html(table_attr['DESCRIPTION']),
                'view_definition': viewdef,
                'view_definition_dbk': docbook(viewdef),

                # lists
                'columns': columns,
                'constraints': constraints,
                'fk_schemas': fk_schemas,
                'indexes': indexes,
                'inherits': inherits,
                'permissions': permissions,
            })

            # only have the count if there is more than 1 schema
            if len(struct) > 1:
                tables[-1]["number_of_schemas"] = len(struct)

        # Dump out list of functions
        functions = list()
        for function in sorted(schema_attr['FUNCTION'].keys() if 'FUNCTION' in schema_attr else []):
            function_attr = schema_attr['FUNCTION'][function]
            functions.append({
                'function': function,
                'function_dbk': docbook(function),
                'function_sgmlid': sgml_safe_id('.'.join((schema, 'function', function))),
                'function_comment': function_attr['COMMENT'],
                'function_comment_dbk': docbook(function_attr['COMMENT']),
                'function_comment_html': html(function_attr['COMMENT']),
                'function_language': function_attr['LANGUAGE'].upper(),
                'function_returns': function_attr['RETURNS'],
                'function_source': function_attr['SOURCE'],
                'schema': schema,
                'schema_dbk': docbook(schema),
                'schema_dot': graphviz(schema),
                'schema_sgmlid': sgml_safe_id(schema + '.schema'),
            })

            # only have the count if there is more than 1 schema
            if len(struct) > 1:
                functions[-1]["number_of_schemas"] = len(struct)

        schemas.append({
            'schema': schema,
            'schema_dbk': docbook(schema),
            'schema_dot': graphviz(schema),
            'schema_sgmlid': sgml_safe_id(schema + '.schema'),
            'schema_comment': schema_attr['SCHEMA']['COMMENT'],
            'schema_comment_dbk': docbook(schema_attr['SCHEMA']['COMMENT']),
            'schema_comment_html': html(schema_attr['SCHEMA']['COMMENT']),

            # lists
            'functions': functions,
            'tables': tables,
        })

        # Build the array of schemas
        if len(struct) > 1:
            schemas[-1]["number_of_schemas"] = len(struct)

    # Link the various components together via the template.
    fk_links = list()
    fkeys = list()
    for schema in sorted(struct.keys()):
        schema_attr = struct[schema]
        for table in sorted(schema_attr['TABLE'].keys() if 'TABLE' in schema_attr else []):
            table_attr = schema_attr['TABLE'][table]
            columnnames = sorted(table_attr['COLUMN'].keys(),
                                 key=lambda column_name: table_attr['COLUMN'][column_name]['ORDER'])
            for column in columnnames:
                column_attr = table_attr['COLUMN'][column]
                for con in sorted(column_attr['CON'].keys() if 'CON' in column_attr else []):
                    con_attr = column_attr['CON'][con]
                    # To prevent a multi-column foreign key from appearing
                    # several times, we've opted
                    # to simply display the first column of any given key.
                    #  Since column numbering always starts at 1
                    # for foreign keys.
                    if con_attr['TYPE'] == 'FOREIGN KEY' and con_attr['COLNUM'] == 1:
                        # Pull out some of the longer keys
                        ref_table = con_attr['FKTABLE']
                        ref_schema = con_attr['FKSCHEMA']
                        ref_column = con_attr['FK-COL NAME']

                        # Default values cause these elements to attach
                        # to the bottom in Dia
                        # If a KEYGROUP is not defined, it's a single column.
                        #  Modify the ref_con and key_con variables to attach
                        # the to the columns connection point directly.
                        ref_con = 0
                        key_con = 0
                        keycon_offset = 0
                        if 'KEYGROUP' not in con_attr:
                            ref_con = struct[ref_schema]['TABLE'][ref_table]['COLUMN'][ref_column]['ORDER']
                            key_con = column_attr['ORDER']
                            keycon_offset = 1

                        # Bump object_id
                        object_id = object_id + 1

                        fk_links.append({
                            'fk_link_name': con,
                            'fk_link_name_dbk': docbook(con),
                            'fk_link_name_dot': graphviz(con),
                            'handle0_connection': key_con,
                            'handle0_connection_dbk': docbook(key_con),
                            'handle0_connection_dia': 6 + (key_con * 2),
                            'handle0_name': table,
                            'handle0_name_dbk': docbook(table),
                            'handle0_schema': schema,
                            'handle0_to': tableids[schema + '.' + table],
                            'handle0_to_dbk': docbook(tableids[schema + '.' + table]),
                            'handle1_connection': ref_con,
                            'handle1_connection_dbk': docbook(ref_con),
                            'handle1_connection_dia': 6 + (ref_con * 2) + keycon_offset,
                            'handle1_name': ref_table,
                            'handle1_name_dbk': docbook(ref_table),
                            'handle1_schema': ref_schema,
                            'handle1_to': tableids[ref_schema + '.' + ref_table],
                            'handle1_to_dbk': docbook(tableids[ref_schema + '.' + ref_table]),
                            'object_id': object_id,
                            'object_id_dbk': docbook(object_id),
                        })

                        # Build the array of schemas
                        if len(struct) > 1:
                            fk_links[-1]["number_of_schemas"] = len(struct)

    # Make database level comment information
    dumped_on = datetime.now().strftime('%Y-%m-%d')
    database_comment = db[database]['COMMENT']
    if database_comment is None:
        database_comment = ''

    # Loop through each template found in the supplied path.
    # Output the results of the template as <filename>.<extension>
    # into the current working directory.
    template_files = list()
    for dir, _, files in os.walk(template_path):
        for file in files:
            if os.path.splitext(file)[1] == '.tmpl':
                template_files.append(os.path.join(dir, file))

    # Ensure we've told the user if we don't find any files.
    if not template_files:
        raise RuntimeError('Templates files not found in {}'.format(template_path))

    # Process all found templates.
    for template_file in template_files:
        file_extension = os.path.splitext(os.path.split(template_file)[1])[0]
        if wanted_output and file_extension != wanted_output:
            continue
        output_filename = output_filename_base + '.' + file_extension
        print('Producing {} from {}'.format(output_filename, template_file))

        template = TemplateManager(debug=0).prepare(template_file)
        tproc = TemplateProcessor(debug=0)

        tproc.set('database', database)
        tproc.set('database_dbk', docbook(database))
        tproc.set('database_sgmlid', sgml_safe_id(database))
        tproc.set('database_comment', database_comment)
        tproc.set('database_comment_dbk', docbook(database_comment))
        tproc.set('database_comment_html', html(database_comment))
        tproc.set('dumped_on', dumped_on)
        tproc.set('dumped_on_dbk', docbook(dumped_on))
        tproc.set('fk_links', fk_links)
        tproc.set('schemas', schemas)

        # Print the processed template.
        with open(output_filename, mode='w') as f:
            f.write(tproc.process(template))


if __name__ == '__main__':
    main()
