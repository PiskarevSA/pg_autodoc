# command line arguments to run in sandbox:
#   -d sandbox -u postgres --password 1 --statistics -t html
#   --host 192.168.12.208 -p 5432 -d radar_db -u postgres --statistics --password=123 -f output/radar_db
#
# profiling:
#   - interpreter options to enable profiling: -B -m cProfile -o output.prof
#   - pip install snakeviz
#   - snakeviz output.prof

import argparse
from datetime import datetime
from htmltmpl import TemplateManager, TemplateProcessor
import json
import os
import psycopg2
import re
import sys
import mako.template
import mako.lookup

import collect_info


def elided(text, left, right):
    mid = ' ... '
    if (left + len(mid) + right) < len(text):
        return text[:left] + mid + text[-right:]
    return text


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


class ProgressBar:
    def __init__(self, title, count):
        self.title = title
        self.current_index = 0
        self.count = count
        self.bar_length = 80
        self.bar_content = self.title + '-' * self.bar_length + ' {} items to process'.format(self.count)
        print(self.bar_content, flush=True, sep='', end='')

    def begin_step(self, about_item):
        bar_left = self.current_index * self.bar_length // self.count
        bar_right = self.bar_length - bar_left
        self.bar_content = self.title + '=' * bar_left + '-' * bar_right + ' item {} of {} in process: {}'.format(
            self.current_index + 1, self.count, about_item)
        print('\r', self.bar_content, flush=True, sep='', end='')
        self.current_index += 1

    def end(self):
        self.bar_content = self.title + '=' * self.bar_length + ' all {} items processed'.format(self.count)
        print('\r', self.bar_content, flush=True, sep='')

    def message(self, *args, sep=' '):
        print('\r', sep='', end='')
        print(*args, sep=sep)
        print(self.bar_content, flush=True, sep='', end='')


def main():
    argv = sys.argv
    db = dict()

    # The templates path
    template_path = 'templates'

    # Setup the default connection variables based on the environment
    dbuser = os.getenv('PGUSER') or os.getenv('USER')
    database = os.getenv('PGDATABASE') or os.getenv('USER')
    dbhost = os.getenv('PGHOST') or 'localhost'
    dbport = os.getenv('PGPORT') or 5432

    # Determine whether we need a password to connect
    needpass = 0
    dbpass = None

    wanted_output = None  # means all types
    statistics = 0

    # Fetch base name
    basename = os.path.split(argv[0])[1]

    parser = argparse.ArgumentParser(
        description='This is a utility which will run through PostgreSQL system tables and returns HTML, DOT, '
                    'and several styles of XML which describe the database. As a result, documentation about a '
                    'project can be generated quickly and be automatically updatable, yet have a quite professional '
                    'look if you do some DSSSL/CSS work.')
    parser.add_argument('-d', metavar='<dbname>', type=str,
                        help='Specify database name to connect to (default: $PGDATABASE or $USER)')
    parser.add_argument('-f', metavar='<file>', type=str,
                        help='Specify output file prefix (default: <dbname>)')
    parser.add_argument('--host', metavar='<host>', type=str,
                        help='Specify database server host (default: $PGHOST or localhost)')
    parser.add_argument('-p', metavar='<port>', type=int,
                        help='Specify database server port (default: $PGPORT or 5432)')
    parser.add_argument('-u', metavar='<username>', type=str,
                        help='Specify database username (default: $PGUSER or $USER)')
    parser.add_argument('--password', metavar='<pw>', type=str,
                        help='Specify database password (default: blank)')
    parser.add_argument('--prompt-password', action="store_true",
                        help='Have {} prompt for a password'.format(basename))
    parser.add_argument('-l', '--library', metavar='<path>', type=str,
                        help='Path to the templates (default: {})'.format(template_path))
    parser.add_argument('-t', '--type', metavar='<output>', type=str,
                        help='Type of output wanted (default: All in template library)')
    parser.add_argument('-c', '--config', metavar='<json>', type=str,
                        help='Config file (default: input/<database>.json). Contains: '
                             '1) whitelist and blacklist regular expressions for schemas; '
                             '2)  whitelist and blacklist regular expressions '
                             'for tables and functions of concrete schema if required')
    parser.add_argument('-w', action="store_true",
                        help='Use ~/.pgpass for authentication (overrides all other password options)')
    parser.add_argument('--statistics', action="store_true",
                        help='In 7.4 and later, with the contrib module pgstattuple installed we can gather '
                             'statistics on the tables in the database (average size, free space, disk space used, '
                             'dead tuple counts, etc.) This is disk intensive on large databases as all pages must be '
                             'visited')
    args = parser.parse_args()

    # Set the database
    if args.d is not None:
        database = args.d
    elif args.u is not None:
        database = args.u

    # Set the user
    if args.u is not None:
        dbuser = args.u

    # Set the hostname
    if args.host is not None:
        dbhost = args.host

    # Set the Port
    if args.p is not None:
        dbport = args.p

    # Set the users password
    if args.password is not None:
        dbpass = args.password

    # Make sure we get a password before attempting to connect
    if args.prompt_password:
        needpass = 1

    # Read from .pgpass (override all other password options)
    if args.w:
        dbpass = None
        dbuser = None
        needpass = 0

    # Set the base of the filename. The extensions pulled
    # from the templates will be appended to this name
    output_filename_base = database
    if args.f is not None:
        output_filename_base = args.f

    # Set the template directory explicitly
    if args.library is not None:
        template_path = args.library

    # Set the output type
    if args.type is not None:
        wanted_output = args.type

    # Read config file
    config_json = os.path.join('input', database + '.json')
    if args.config is not None:
        config_json = args.config
    with open(config_json) as config_json_file:
        config_data = json.load(config_json_file)
        schemas_whitelist_regex = config_data.get('schemas_whitelist_regex')
        schemas_blacklist_regex = config_data.get('schemas_blacklist_regex')
        schema_tweaks = config_data.get('schema_tweaks')
        layers = config_data.get('layers')
        services = config_data.get('services', dict())

    layers_url = dict()
    if layers:
        url = layers.get("url", "")
        arguments_names = layers.get("arguments_names", list())
        arguments_values = layers.get("arguments_values", dict())
        for layer, layer_arguments_values in arguments_values.items():
            layer_url = url
            first_arg = True
            for name, value in zip(arguments_names, layer_arguments_values):
                layer_url += ('?' if first_arg else '&') + name + '=' + value
                first_arg = False
            layers_url[layer.lower()] = {"name": layer, "url": layer_url}

    services_url = dict()
    for service_name, service_url in services.items():
        services_url[service_name.lower()] = {"name": service_name, "url": service_url}

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
    conn = psycopg2.connect(database=database, user=dbuser, password=dbpass, host=dbhost, port=dbport)
    conn.set_client_encoding('UTF8')

    info_collect(conn, db, database, schemas_whitelist_regex, schemas_blacklist_regex, schema_tweaks, statistics)
    conn.close()

    output_filename = output_filename_base + '.json'
    with open(output_filename, 'w') as outfile:
        json.dump(db, outfile, indent=2, cls=collect_info.PgJsonEncoder)

    info_postprocess(db, layers_url, services_url)

    output_filename = output_filename_base + '.postprocessed.json'
    with open(output_filename, 'w') as outfile:
        json.dump(db, outfile, indent=2, cls=collect_info.PgJsonEncoder)

    # Write out *ALL* templates
    write_using_templates(db, database, template_path, output_filename_base, wanted_output)


##
# info_collect
#
# Pull out all of the applicable information about a specific database
def info_collect(conn, db, database, schemas_whitelist_regex, schemas_blacklist_regex, schema_tweaks, statistics):
    print('collecting data')
    if schemas_whitelist_regex is None:
        schemas_whitelist_regex = '^'
    if schemas_blacklist_regex is None:
        schemas_blacklist_regex = '^'
    if schema_tweaks is None:
        schema_tweaks = dict()

    db[database] = dict()
    struct = db[database]['STRUCT'] = dict()

    # PostgreSQL's version is used to determine what queries are required
    # to retrieve a given information set.
    if conn.server_version < 70400:
        raise RuntimeError("PostgreSQL 7.4 and later are supported")

    # Ensure we only retrieve information for the requested schemas.
    #
    # system_schema           -> The primary system schema for a database.
    #
    # schemas_whitelist_regex -> The list of schemas which we are supposed
    #                            to gather information for, lower priority than blacklist
    #
    # schemas_blacklist_regex -> The list of schemas which we are not supposed
    #                            to gather information for, higher priority than whitelist
    #
    # schema_tweaks           -> The individual schema tweaks:
    #  .. tables_whitelist_regex     -> gather info if not None and table name matched regular expressions
    #  .. tables_blacklist_regex:    -> gather info if not None and table name not matched regular expressions
    #  .. functions_whitelist_regex: -> gather info if not None and function name matched regular expressions
    #  .. functions_blacklist_regex: -> gather info if not None and function name not matched regular expressions
    system_schema = 'pg_catalog'

    cur = conn.cursor()

    # Fetch Database info
    db[database]['COMMENT'] = collect_info.get_database_description(cur, database)

    # Fetch list of schemas
    schemas = collect_info.get_schemas(cur, schemas_whitelist_regex, schemas_blacklist_regex)

    # Fetch tables and all things bound to tables
    tables = list()
    for schema in schemas:
        tables_whitelist_regex = tables_blacklist_regex = None
        if schema in schema_tweaks:
            tables_whitelist_regex = schema_tweaks[schema].get('tables_whitelist_regex')
            tables_blacklist_regex = schema_tweaks[schema].get('tables_blacklist_regex')
        tables += collect_info.get_tables(cur, schema, tables_whitelist_regex, tables_blacklist_regex)

    permission_flag_to_str = {
        'a': 'INSERT',
        'r': 'SELECT',
        'w': 'UPDATE',
        'd': 'DELETE',
        'R': 'RULE',
        'x': 'REFERENCES',
        't': 'TRIGGER',
    }

    table_bar = ProgressBar('tables:    ', len(tables))
    for (item_index, table) in enumerate(tables):
        table_bar.begin_step(table['tablename'])

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
            stats = collect_info.get_statistics(cur, reloid)
            assert len(stats) == 1
            set_table_attribute(struct, schema, relname, 'HAS_STATISTICS', True)
            set_table_attribute(struct, schema, relname, 'TABLELEN', stats[0]['table_len'])
            set_table_attribute(struct, schema, relname, 'TUPLECOUNT', stats[0]['tuple_count'])
            set_table_attribute(struct, schema, relname, 'TUPLELEN', stats[0]['tuple_len'])
            set_table_attribute(struct, schema, relname, 'DEADTUPLELEN', stats[0]['dead_tuple_len'])
            set_table_attribute(struct, schema, relname, 'FREELEN', stats[0]['free_space'])
        else:
            set_table_attribute(struct, schema, relname, 'HAS_STATISTICS', False)

        # Store the relation type
        set_table_attribute(struct, schema, relname, 'TYPE', table['reltype'])

        # Store table description
        set_table_attribute(struct, schema, relname, 'DESCRIPTION', table['table_description'])

        # Store the view definition
        set_table_attribute(struct, schema, relname, 'VIEW_DEF', table['view_definition'])

        # Store constraints
        constraints = collect_info.get_constraint(cur, reloid)
        for constraint in constraints:
            constraint_name = constraint['constraint_name']
            constraint_source = constraint['constraint_source']
            set_constraint(struct, schema, relname, constraint_name, constraint_source)

        columns = collect_info.get_columns(cur, reloid)
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
        primary_keys = collect_info.get_primary_keys(cur, reloid)
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
        foreign_keys = collect_info.get_foreign_keys(cur, reloid, schemas)
        fkgroup = 0
        for forcols in foreign_keys:
            column_oid = forcols['oid']
            con = forcols['constraint_name']

            # Declare variables for dataload
            keylist = list()
            fkeylist = list()
            fschema = None
            ftable = None

            fkeyset = forcols['constraint_fkey']
            keyset = forcols['constraint_key']
            frelid = forcols['foreignrelid']

            # Convert the list of column numbers into column names for the
            # local side.
            for k in keyset:
                foreign_key_arg = collect_info.get_foreign_key_arg(cur, reloid, k)
                assert len(foreign_key_arg) == 1
                keylist.append(foreign_key_arg[0]['attribute_name'])

            # Convert the list of columns numbers into column names
            # for the referenced side. Grab the table and namespace
            # while we're here.
            for k in fkeyset:
                foreign_key_arg = collect_info.get_foreign_key_arg(cur, frelid, k)
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
        indexes = collect_info.get_indexes(cur, schema, relname)
        for idx in indexes:
            index_name = idx['indexname']
            index_definition = idx['indexdef']
            set_index_definition(struct, schema, relname, index_name, index_definition)

        # Extract Inheritance information
        inheritance = collect_info.get_inheritance(cur, schema, relname, schemas)
        for inherit in inheritance:
            parent_schemaname = inherit['par_schemaname']
            parent_tablename = inherit['par_tablename']
            set_table_inherit(struct, schema, relname, parent_schemaname, parent_tablename)

    table_bar.end()

    # Function Handling
    functions = list()
    for schema in schemas:
        functions_whitelist_regex = functions_blacklist_regex = None
        if schema in schema_tweaks:
            functions_whitelist_regex = schema_tweaks[schema].get('functions_whitelist_regex')
            functions_blacklist_regex = schema_tweaks[schema].get('functions_blacklist_regex')
        functions += collect_info.get_functions(cur, schema, functions_whitelist_regex, functions_blacklist_regex)

    function_bar = ProgressBar('functions: ', len(functions))
    for function_index, function in enumerate(functions):
        function_bar.begin_step(function['function_name'])
        schema = function['namespace']
        comment = function['comment']
        functionargs = function['function_args']
        types = functionargs.split()

        # Pre-setup argument names when available.
        argnames = function['function_arg_names']

        # Setup full argument types including the parameter name
        parameters = list()
        for type_oid in types:
            function_arg = collect_info.get_function_arg(cur, type_oid)
            assert len(function_arg) == 1
            parameter = argnames.pop(0) + ' ' if argnames else ''
            if function_arg[0]['namespace'] != system_schema:
                parameter = parameter + function_arg[0]['namespace'] + '.'
            parameter = parameter + function_arg[0]['type_name']
            parameters.append(parameter)
        functionname = '{}({})'.format(function['function_name'], ', '.join(parameters))

        ret_type = 'SET OF ' if function['returns_set'] else ''
        function_arg = collect_info.get_function_arg(cur, function['return_type'])
        assert len(function_arg) == 1
        ret_type = ret_type + function_arg[0]['type_name']

        set_function_attribute(struct, schema, functionname, 'NAME', function['function_name'])
        set_function_attribute(struct, schema, functionname, 'ARGS', parameters)
        set_function_attribute(struct, schema, functionname, 'COMMENT', comment)
        set_function_attribute(struct, schema, functionname, 'SOURCE', function['source_code'])
        set_function_attribute(struct, schema, functionname, 'LANGUAGE', function['language_name'])
        set_function_attribute(struct, schema, functionname, 'RETURNS', ret_type)

    function_bar.end()

    # Deal with the Schema
    schema_comments = collect_info.get_schemas_comment(cur, schemas)
    for schema_comment in schema_comments:
        comment = schema_comment['comment']
        namespace = schema_comment['namespace']
        set_schema_comment(struct, namespace, comment)

    cur.close()


class CommentsParser:
    def __init__(self, db, layers_url, services_url):
        self.db = db
        self.layers_url = layers_url
        self.services_url = services_url
        self.tables = dict()
        self.functions = dict()

    def parse(self):
        self.__collect_objects()
        self.__parse_and_analyse_comments()

    def __collect_objects(self):
        for database in self.db:
            db_tables = self.tables[database] = dict()
            db_functions = self.functions[database] = dict()
            schemas = self.db[database]['STRUCT']
            for schema, schema_attr in schemas.items():
                # .. tables
                schema_tables = db_tables[schema] = set()
                src_tables = schema_attr.get('TABLE', dict())
                for table in src_tables:
                    schema_tables.add(table)
                # .. functions, keep only first (in alphabetical order) set of arguments
                schema_functions = db_functions[schema] = dict()
                src_functions = schema_attr.get('FUNCTION', dict())
                for function in sorted(src_functions.keys(), reverse=True):
                    schema_functions[src_functions[function]['NAME']] = function

    def __parse_and_analyse_comments(self):
        for database in self.db:
            schemas = self.db[database]['STRUCT']
            for schema, schema_attr in schemas.items():
                # .. tables
                tables = schema_attr.get('TABLE', dict())
                for table, table_attr in tables.items():
                    self.__postprocess_table_description(database, table_attr)
                # .. functions
                functions = schema_attr.get('FUNCTION', dict())
                for function, function_attr in functions.items():
                    self.__postprocess_function_comment(database, function_attr)

    def __postprocess_table_description(self, database, table_attr: dict):
        table_description = table_attr['DESCRIPTION']
        if table_description is None:
            return
        match = re.finditer(r'\\\w+', table_description)
        for m in match:
            keyword = dict()
            table_attr.setdefault('KEYWORDS', list()).append(keyword)
            keyword['NAME'] = m.group()
            keyword['POSITION'] = m.start()
            keyword['LENGTH_WITH_ARGS'] = keyword['LENGTH'] = len(m.group())

            if keyword['NAME'] in ('\\depends', '\\affects'):
                elements = self.__parse_keyword_depends_or_affects(table_description, m.start())
                if elements is not None:
                    keyword['LENGTH_WITH_ARGS'], keyword['ARGS'] = elements
                    error = self.__check_depends_or_affects_target_exists(database, keyword)
                    if error is not None:
                        keyword['ERROR'] = error
                else:
                    keyword['ERROR'] = 'ARGS_PARSE_ERROR'
            else:
                keyword['ERROR'] = 'UNEXPECTED_KEYWORD'

    def __postprocess_function_comment(self, database, function_attr: dict):
        comment = function_attr['COMMENT']
        if comment is None:
            return
        match = re.finditer(r'\\\w+', comment)
        for m in match:
            keyword = dict()
            function_attr.setdefault('KEYWORDS', list()).append(keyword)
            keyword['NAME'] = m.group()
            keyword['POSITION'] = m.start()
            keyword['LENGTH_WITH_ARGS'] = keyword['LENGTH'] = len(m.group())

            if keyword['NAME'] in ('\\depends', '\\affects'):
                elements = self.__parse_keyword_depends_or_affects(comment, m.start())
                if elements is not None:
                    keyword['LENGTH_WITH_ARGS'], keyword['ARGS'] = elements
                    error = self.__check_depends_or_affects_target_exists(database, keyword)
                    if error is not None:
                        keyword['ERROR'] = error
                else:
                    keyword['ERROR'] = 'ARGS_PARSE_ERROR'
            elif keyword['NAME'] == '\\param':
                elements = self.__parse_keyword_param(comment, m.start())
                if elements is not None:
                    keyword['LENGTH_WITH_ARGS'], keyword['ARGS'] = elements
                else:
                    keyword['ERROR'] = 'ARGS_PARSE_ERROR'
            else:
                keyword['ERROR'] = 'UNEXPECTED_KEYWORD'

    def __check_depends_or_affects_target_exists(self, database, keyword):
        args = keyword['ARGS']
        target_object_type = args['OBJECT_TYPE']['VALUE']
        target_schema = args['SCHEMA']['VALUE']
        target_object = args['OBJECT']['VALUE']

        target_schema_lc = target_schema.lower() if target_schema is not None else None
        target_object_lc = target_object.lower()

        schemas = self.db[database].get('STRUCT', dict())

        if target_object_type in ('TABLE', 'VIEW', 'MATVIEW'):
            if target_schema is None:
                return 'SCHEMA_REQUIRED'
            if target_schema_lc not in schemas:
                return 'NO_SUCH_SCHEMA'
            if target_object_lc not in self.tables[database].get(target_schema_lc, set()):
                return 'NO_SUCH_TABLE_OR_VIEW'
            target = schemas[target_schema_lc]['TABLE'][target_object_lc]
            keyword['TARGET_TYPE'] = target['TYPE'].upper()
            return None
        elif target_object_type == 'FUNCTION':
            if target_schema is None:
                return 'SCHEMA_REQUIRED'
            if target_schema_lc not in schemas:
                return 'NO_SUCH_SCHEMA'
            if target_object_lc not in self.functions[database].get(target_schema_lc, dict()):
                return 'NO_SUCH_FUNCTION'
            keyword['TARGET_TYPE'] = 'FUNCTION'
            keyword['FUNCTION_WITH_ARGS'] = self.functions[database][target_schema_lc][target_object_lc]
            return None
        elif target_object_type == 'LAYER':
            if target_object_lc not in self.layers_url:
                return 'NO_SUCH_LAYER'
            keyword['TARGET_TYPE'] = 'LAYER'
            keyword['LAYER_NAME'] = self.layers_url[target_object_lc]['name']
            keyword['LAYER_URL'] = self.layers_url[target_object_lc]['url']
            return None
        elif target_object_type == 'SERVICE':
            if target_object_lc not in self.services_url:
                return 'NO_SUCH_SERVICE'
            keyword['TARGET_TYPE'] = 'SERVICE'
            keyword['SERVICE_NAME'] = self.services_url[target_object_lc]['name']
            keyword['SERVICE_URL'] = self.services_url[target_object_lc]['url']
            return None
        else:
            return 'UNEXPECTED_OBJECT_TYPE'

    def __arg_from_rx(self, keyword_start, match, group):
        arg = dict()
        arg['VALUE'] = match.group(group)
        arg['POSITION'] = keyword_start + match.start(group)
        arg['LENGTH'] = len(arg['VALUE']) if arg['VALUE'] is not None else 0
        return arg

    def __parse_keyword_depends_or_affects(self, comment, keyword_start):
        mm = re.match(r'^\\(?:depends|affects)\s+(\w+):\s*(?:(\w+)\.)?(\w+)', comment[keyword_start:])
        if mm is not None:
            args = dict()
            args['OBJECT_TYPE'] = self.__arg_from_rx(keyword_start, mm, 1)
            args['SCHEMA'] = self.__arg_from_rx(keyword_start, mm, 2)
            args['OBJECT'] = self.__arg_from_rx(keyword_start, mm, 3)
            return len(mm.group()), args
        else:
            return None

    def __parse_keyword_param(self, comment, keyword_start):
        mm = re.match(r'^\\param\s+(\w+)', comment[keyword_start:])
        if mm is not None:
            args = dict()
            args['PARAM_NAME'] = self.__arg_from_rx(keyword_start, mm, 1)
            return len(mm.group()), args
        else:
            return None


class DependenciesInvestigator:
    def __init__(self, db):
        self.db = db

    def investigate(self):
        for database in self.db:
            schemas = self.db[database]['STRUCT']
            dependencies = list()
            for schema, schema_attr in schemas.items():
                # .. tables
                tables = schema_attr.get('TABLE', dict())
                for tablename, table in tables.items():
                    self.__analyse_keywords(table['TYPE'], schema, tablename, table.get('KEYWORDS', list()), dependencies)
                # .. functions
                functions = schema_attr.get('FUNCTION', dict())
                for functionname, function in functions.items():
                    self.__analyse_keywords('FUNCTION', schema, functionname, function.get('KEYWORDS', list()), dependencies)
            tree_root_node = self.db[database]['DEPENDENCIES'] = dict()
            self.__build_tree(dependencies, tree_root_node, lambda root_node: root_node['TYPE'] in ('LAYER', 'SERVICE'))

    def __analyse_keywords(self, source_type, source_schema, source_object, keywords, dependencies):
        for keyword in keywords:
            if keyword['NAME'] in ('\\depends', '\\affects'):
                source = {
                    'TYPE': source_type.upper(),
                    'SCHEMA': source_schema,
                    'OBJECT': source_object,
                }
                target = {
                    'TYPE': keyword['ARGS']['OBJECT_TYPE']['VALUE'],
                    'SCHEMA': keyword['ARGS']['SCHEMA']['VALUE'],
                    'OBJECT': keyword['ARGS']['OBJECT']['VALUE']
                }
                if 'TARGET_TYPE' in keyword:
                    target['TYPE'] = keyword['TARGET_TYPE']
                if 'FUNCTION_WITH_ARGS' in keyword:
                    target['OBJECT'] = keyword['FUNCTION_WITH_ARGS']
                if 'LAYER_NAME' in keyword:
                    target['OBJECT'] = keyword['LAYER_NAME']
                if 'SERVICE_NAME' in keyword:
                    target['OBJECT'] = keyword['SERVICE_NAME']
                for optional_field in ('ERROR', 'LAYER_URL', 'SERVICE_URL'):
                    if optional_field in keyword:
                        target[optional_field] = keyword[optional_field]

                def make_id(object):
                    object_type = object['TYPE'].lower()
                    object_schema = '' if object['SCHEMA'] is None else object['SCHEMA'].lower()
                    object_name = object['OBJECT'].lower()
                    object['ID'] = '.'.join((object_type, object_schema, object_name))

                make_id(source)
                make_id(target)
                if keyword['NAME'] == '\\affects':
                    source, target = target, source
                dependencies.append((source, target))

    def __build_tree(self, dependencies, tree_root_node: dict, root_predictate):
        for source, target in dependencies:
            if root_predictate(source):
                if source['ID'] not in tree_root_node:
                    node = tree_root_node[source['ID']] = dict()
                    node['ATTR'] = source
                else:
                    node = tree_root_node[source['ID']]
                node_childs = node.setdefault('CHILDS', dict())
                child_id = target['ID']
                assert child_id not in node_childs
                child_node = node_childs[child_id] = dict()
                child_node['ATTR'] = target
                self.__add_childs(dependencies, child_node)

    def __add_childs(self, dependencies, parent_node):
        for source, target in dependencies:
            if source['ID'] == parent_node['ATTR']['ID']:
                node_childs = parent_node.setdefault('CHILDS', dict())
                child_id = target['ID']
                assert child_id not in node_childs
                child_node = node_childs[child_id] = dict()
                child_node['ATTR'] = target
                self.__add_childs(dependencies, child_node)


def info_postprocess(db, layers_url, services_url):
    print('postprocessing data')
    comments_parser = CommentsParser(db, layers_url, services_url)
    comments_parser.parse()
    dependencies_investigator = DependenciesInvestigator(db)
    dependencies_investigator.investigate()


######
# sgml_safe_id
#   Safe SGML ID Character replacement
def sgml_safe_id(string):
    # Lets use the keyword ARRAY in place of the square brackets
    # to prevent duplicating a non-array equivelent
    string = re.sub('\\[\\]', 'ARRAY-', string)

    # Brackets, spaces, commas, underscores are not valid 'id' characters
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


def make_comment_html(comment, is_function_comment: bool, keywords: list):
    if comment is None:
        return None
    result = str()
    comment_pos = 0
    for keyword in keywords:
        # .. add to result comment part before current keyword
        keyword_pos = keyword['POSITION']
        keyword_with_args_end = keyword_pos + keyword['LENGTH_WITH_ARGS']
        if keyword_pos != comment_pos:
            result += html(comment[comment_pos:keyword_pos])
            comment_pos = keyword_pos
        # .. insert error if exists
        if 'ERROR' in keyword:
            result += '<b><font color="red">[ERROR: {}] </font></b>'.format(keyword['ERROR'])
            result += comment[keyword_pos:keyword_with_args_end]
            comment_pos = keyword_with_args_end
            continue
        # .. \depends, \affects: insert hyperlink if exists
        if keyword['NAME'] in ('\\depends', '\\affects'):
            is_depends = keyword['NAME'] == '\\depends'
            object_type = keyword['TARGET_TYPE']
            schema_name = keyword['ARGS']['SCHEMA']['VALUE']
            if schema_name:
                schema_name = schema_name.lower()
            object_name = keyword['ARGS']['OBJECT']['VALUE'].lower()
            full_object_name = schema_name + '.' + object_name if schema_name else object_name

            add_inner_reference = False
            outer_reference = None
            if object_type == 'FOREIGN TABLE':
                prefix = 'Зависит от внешней таблицы' if is_depends else 'Влияет на внешнюю таблицу'
                add_inner_reference = True
            elif object_type == 'MATERIALIZED VIEW':
                prefix = 'Зависит от материального представления' if is_depends else 'Влияет на материальное представление'
                add_inner_reference = True
            elif object_type == 'SPECIAL':
                prefix = 'Зависит от специальной таблицы' if is_depends else 'Влияет на специальную таблицу'
                add_inner_reference = True
            elif object_type == 'TABLE':
                prefix = 'Зависит от таблицы' if is_depends else 'Влияет на таблицу'
                add_inner_reference = True
            elif object_type == 'VIEW':
                prefix = 'Зависит от представления' if is_depends else 'Влияет на представление'
                add_inner_reference = True
            elif object_type == 'FUNCTION':
                prefix = 'Зависит от функции' if is_depends else 'Влияет на функцию'
                object_name = keyword['FUNCTION_WITH_ARGS']
                add_inner_reference = True
            elif object_type == 'LAYER':
                prefix = 'Зависит от слоя' if is_depends else 'Влияет на слой'
                full_object_name = keyword['LAYER_NAME']
                outer_reference = keyword['LAYER_URL']
            elif object_type == 'SERVICE':
                prefix = 'Зависит от сервиса' if is_depends else 'Влияет на сервис'
                full_object_name = keyword['SERVICE_NAME']
                outer_reference = keyword['SERVICE_URL']
            else:
                raise RuntimeError('unexpected object type: {}'.format(object_type))
            if add_inner_reference:
                inner_reference = sgml_safe_id('.'.join((schema_name, object_type.lower(), object_name)))
                html_target = '{} <a href=#{}>{}</a>'.format(prefix, inner_reference, full_object_name)
            elif outer_reference:
                html_target = '{} <a href={}>{}</a>'.format(prefix, outer_reference, full_object_name)
            else:
                html_target = prefix + ' ' + full_object_name
            result += html_target
            comment_pos = keyword_with_args_end
            continue
        # .. \param (function only): bold param
        if is_function_comment and keyword['NAME'] == '\\param':
            html_target = 'Параметр: <b>{}</b>'.format(keyword['ARGS']['PARAM_NAME']['VALUE'])
            result += html_target
            comment_pos = keyword_with_args_end
            continue

        raise RuntimeError('unexpected keyword')

    if comment_pos != len(comment):
        result += html(comment[comment_pos:len(comment)])
    return result


def make_table_comment_html(comment: str, keywords: list):
    return make_comment_html(comment, False, keywords)


def make_function_comment_html(comment: str, keywords: list):
    return make_comment_html(comment, True, keywords)


#####
# write_using_templates
#
# Generate structure that HTML::Template requires out of the
# 'STRUCT' for table related information, and 'STRUCT' for
# the schema and function information
def write_using_templates(db, database, template_path, output_filename_base, wanted_output):
    print('write using templates')
    struct = db[database]['STRUCT']

    schemas = list()

    # Foreign Key Discovery
    foreign_keys = dict()
    for fk_schema in sorted(struct.keys()):
        fk_schema_attr = struct[fk_schema]
        for fk_table in sorted(fk_schema_attr['TABLE'] if 'TABLE' in fk_schema_attr else []):
            fk_table_attr = fk_schema_attr['TABLE'][fk_table]
            for fk_column in sorted(fk_table_attr['COLUMN'] if 'COLUMN' in fk_table_attr else []):
                fk_column_attr = fk_table_attr['COLUMN'][fk_column]
                for fk_con in sorted(fk_column_attr['CON'] if 'CON' in fk_column_attr else []):
                    con_attr = fk_column_attr['CON'][fk_con]
                    if con_attr['TYPE'] == 'FOREIGN KEY':
                        table = con_attr['FKTABLE']
                        schema = con_attr['FKSCHEMA']
                        fksgmlid = sgml_safe_id('.'.join((fk_schema, fk_table_attr['TYPE'], fk_table)))
                        table_foreign_keys = foreign_keys.setdefault(schema, dict()).setdefault(table, list())
                        table_foreign_keys.append({
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
                            table_foreign_keys[-1]["number_of_schemas"] = len(struct)

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
                    shortdefault = elided(shortdefault, 17, 5)

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
                shortcon = elided(shortcon, 30, 5)
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

            # Foreign Keys
            table_foreign_keys = foreign_keys.get(schema, dict()).get(table, list())

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
                comment_dia = elided(comment_dia, 35, 5)

            def table_stat_attr(name):
                return table_attr[name] if name in table_attr else None

            keywords = table_attr.get('KEYWORDS', list())
            table_comment_html = make_table_comment_html(table_attr['DESCRIPTION'], keywords)

            stats_enabled = table_stat_attr('HAS_STATISTICS')
            tables.append({
                'object_id': object_id,
                'object_id_dbk': docbook(object_id),

                'schema': schema,
                'schema_dbk': docbook(schema),
                'schema_dot': graphviz(schema),
                'schema_sgmlid': sgml_safe_id(schema + '.schema'),

                # Statistics
                'stats_enabled': stats_enabled,

                'table': table,
                'table_dbk': docbook(table),
                'table_dot': graphviz(table),
                'table_type': table_attr['TYPE'],
                'table_type_dbk': docbook(table_attr['TYPE']),
                'table_sgmlid': sgml_safe_id('.'.join((schema, table_attr['TYPE'], table))),
                'table_comment': table_attr['DESCRIPTION'],
                'table_comment_dbk': docbook(table_attr['DESCRIPTION']),
                'table_comment_dia': comment_dia,
                'table_comment_html': table_comment_html,
                'view_definition': viewdef,
                'view_definition_dbk': docbook(viewdef),

                # lists
                'columns': columns,
                'constraints': constraints,
                'fk_schemas': table_foreign_keys,
                'indexes': indexes,
                'inherits': inherits,
                'permissions': permissions,
            })

            if stats_enabled:
                tables[-1]['stats_dead_bytes'] = use_units(table_stat_attr('DEADTUPLELEN'))
                tables[-1]['stats_dead_bytes_dbk'] = docbook(use_units(table_stat_attr('DEADTUPLELEN')))
                tables[-1]['stats_free_bytes'] = use_units(table_stat_attr('FREELEN'))
                tables[-1]['stats_free_bytes_dbk'] = docbook(use_units(table_stat_attr('FREELEN')))
                tables[-1]['stats_table_bytes'] = use_units(table_stat_attr('TABLELEN'))
                tables[-1]['stats_table_bytes_dbk'] = docbook(use_units(table_stat_attr('TABLELEN')))
                tables[-1]['stats_tuple_count'] = table_stat_attr('TUPLECOUNT')
                tables[-1]['stats_tuple_count_dbk'] = docbook(table_stat_attr('TUPLECOUNT'))
                tables[-1]['stats_tuple_bytes'] = use_units(table_stat_attr('TUPLELEN'))
                tables[-1]['stats_tuple_bytes_dbk'] = docbook(use_units(table_stat_attr('TUPLELEN')))

            # only have the count if there is more than 1 schema
            if len(struct) > 1:
                tables[-1]["number_of_schemas"] = len(struct)

        # Dump out list of functions
        functions = list()
        for function in sorted(schema_attr['FUNCTION'].keys() if 'FUNCTION' in schema_attr else []):
            function_attr = schema_attr['FUNCTION'][function]
            keywords = function_attr.get('KEYWORDS', list())
            function_comment_html = make_function_comment_html(function_attr['COMMENT'], keywords)
            functions.append({
                'function': function,
                'function_dbk': docbook(function),
                'function_sgmlid': sgml_safe_id('.'.join((schema, 'function', function))),
                'function_comment': function_attr['COMMENT'],
                'function_comment_dbk': docbook(function_attr['COMMENT']),
                'function_comment_html': function_comment_html,
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

    def make_html_dependencies(dependencies, root=None):
        if not dependencies:
            return None
        template_lookup = mako.lookup.TemplateLookup(directories=['templates'])
        template = template_lookup.get_template('make_html_dependencies.mako')
        return template.render(dependencies=dependencies)

    dependencies = db[database]['DEPENDENCIES']
    html_dependencies = make_html_dependencies(dependencies)

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
        tproc.set('dependencies', html_dependencies)

        # Print the processed template.
        with open(output_filename, mode='w') as f:
            f.write(tproc.process(template))


if __name__ == '__main__':
    main()
