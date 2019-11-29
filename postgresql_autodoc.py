# command line arguments to run in sandbox:
#   postgresql_autodoc.py -d sandbox -u postgres --password 1 -t html
import argparse
import os
import sys


def main():
    argv = sys.argv
    db = dict()

    # The templates path
    template_path = '/usr/local/share/postgresql_autodoc'

    # Setup the default connection variables based on the environment
    dbuser = os.getenv('PGUSER') or os.getenv('USER')
    database = os.getenv('PGDATABASE') or os.getenv('USER')
    dbhost = os.getenv('PGHOST') or ''
    dbport = os.getenv('PGPORT') or ''

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
    if args.prompt_password is not None:
        needpass = 1

    # Read from .pgpass (override all other password options)
    if args.w is not None:
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
    if args.statistics is not None:
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


if __name__ == '__main__':
    main()
