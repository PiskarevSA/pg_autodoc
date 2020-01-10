==================
PostgreSQL Autodoc
==================

This is a utility which will run through PostgreSQL system tables and
returns HTML, DOT, and several styles of XML which describe the
database.

As a result, documentation about a project can be generated quickly
and be automatically updatable, yet have a quite professional look if
you do some DSSSL/CSS work.

Requirements
============

- PostgreSQL 7.4 or later
- python3 with the following packages:
    - psycopg2
    - Mako

Usage
=====

::

    postgresql_autodoc.py [-h] [-d <dbname>] [-f <file>] [--host <host>]
                          [-p <port>] [-u <username>] [--password <pw>]
                          [--prompt-password] [-l <path>] [-t <output>]
                          [-c <json>] [-w] [--statistics]

Options
-------

    - ``-d <dbname>``
        Specify database name to connect to (default: $PGDATABASE or $USER)
    - ``-f <file>``
        Specify output file prefix (default: <dbname>)
    - ``--host <host>``
        Specify database server host (default: $PGHOST or localhost)
    - ``-p <port>``
        Specify database server port (default: $PGPORT or 5432)
    - ``-u <username>``
        Specify database username (default: $PGUSER or $USER)
    - ``--password '<pw>'``
        Specify database password (default: blank)
    - ``--prompt-password``
        Have *py_autodoc* prompt for a password
    - ``[-l|--library] <path>``
        Path to the templates (default: templates)
    - ``[-t|--type] <output>``
        Type of output wanted (default: All in template library)

        - html
            The HTML is human readable (via web browser), representing the entire schema within a single HTML document,
            and includes referenceable labels for each object.
        - dia
            This remaps the schema into XML using the XML schema of [[https://git.gnome.org/browse/dia/][Dia]],
            an interactive diagramming tool.  It does not do any automated layout, so making the diagram usable would
            require manual work, so this is often not terribly useful.
        - xml
            The second type of XML is similar to HTML, but is in DocBook 4 format. It enables you to mix schema
            documentation with other DocBook documentation via the XREFs, generating PDFs, HTML, RTF, or other
            formatted documents.  Object references can be made between these tools and JavaDoc with use of
            appropriate XREFs (see ~xreflabel~ elements in the XML).
        - dot, neato
            This generates the schema in the form accepted by *GraphViz dot* and *neato* respectively, which draws
            the schema as an undirected graph
        - zigzag.dia
            This generates a diagram for Dia in another form
    - ``[-c|--config] <json>``
        Config file (default: input/<database>.json). Contains:

            1) whitelist and blacklist regular expressions for schemas;
            2) whitelist and blacklist regular expressions for tables and functions of concrete schema if required.
    - ``-w``
        Use ~/.pgpass for authentication (overrides all other password options)
    - ``--statistics``
        help='With the contrib module **pgstattuple** installed we can gather statistics on the tables
        in the database (average size, free space, disk space used, dead tuple counts, etc.) This is disk intensive
        on large databases as all pages must be visited

Authors
=======

 - `original Perl version`_ (uses HTML::Template)
    - Rod Taylor <autodoc@rbt.ca>

 - python3 version  (uses Mako)
    - Semen Piskarev <piskarevsa@mail.ru>

.. _original Perl version: https://github.com/cbbrowne/autodoc

License
=======
LGPLv3_

.. _LGPLv3: https://opensource.org/licenses/lgpl-3.0.html