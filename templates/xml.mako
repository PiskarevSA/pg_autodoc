<?xml version="1.0" encoding="UTF-8" ?>
<!-- $Header: /cvsroot/autodoc/autodoc/xml.tmpl,v 1.3 2012/01/05 15:22:28 rbt Exp $ -->
<!DOCTYPE book PUBLIC "-//OASIS//DTD DocBook XML V4.5//EN"
               "http://www.oasis-open.org/docbook/xml/4.5/docbookx.dtd" [
<!ENTITY % myent SYSTEM "entities.ent">
%%myent;
]>
<book id="database.${database_sgmlid}" xreflabel="${database_dbk} database schema"><title>${database_dbk} Model</title>

% if database_comment:
${database_comment_dbk}
% endif

% for schema in schemas:
  <chapter id="${schema['schema_sgmlid']}"
           xreflabel="${schema['schema_dbk']}">
    <title>Schema ${schema['schema_dbk']}</title>
    <para>${schema['schema_comment_dbk']}</para>

  % for table in schema['tables']:
      <section id="${table['table_sgmlid']}"
               xreflabel="${table['schema_dbk']}.${table['table_dbk']}">
        <title id="${table['table_sgmlid']}-title">
${table['table_type_dbk']}:
         <database class="${table['table_type_dbk']}">${table['table_dbk']}</database>
        </title>

    % if table['table_comment']:
        <para>
${table['table_comment_dbk']}
        </para>
    % endif

        <para>
          <variablelist>
            <title>
              Structure of <database class="table">${table['table_dbk']}</database>
            </title>

    % for column in table['columns']:
            <varlistentry>
              <term><database class="field">${column['column_dbk']}</database></term>
              <listitem><para>
                <database class="datatype">${column['column_type_dbk']}</database>
      % for column_constraint in column['column_constraints']:
        % if 'column_primary_key' in column_constraint:
                <database class="constraint">PRIMARY KEY</database>
        % endif

        % if 'column_unique' in column_constraint:
                <database class="constraint">UNIQUE\
          % if column_constraint['column_unique_keygroup']:
#${column_constraint['column_unique_keygroup']}\
          % endif
</database>
        % endif
      % endfor

      % if column.get('column_constraint_notnull'):
                <database class="constraint">NOT NULL</database>
      % endif

      % if column['column_default']:
                <literal>DEFAULT ${column['column_default_dbk']}</literal>
      % endif

      % for column_constraint in column['column_constraints']:
        % if 'column_fk' in column_constraint:
                <database class="constraint">REFERENCES <xref linkend="${column_constraint['column_fk_sgmlid']}"/></database>
        % endif
      % endfor
              </para>
      % if column['column_comment']:
              <para>${column['column_comment_dbk']}</para>
      % endif
            </listitem>
          </varlistentry>
    % endfor
        </variablelist>

    % for index, constraint in enumerate(table['constraints']):
      % if index == 0:
        <variablelist>
          <title>Constraints on ${constraint['table_dbk']}</title>
      % endif
          <varlistentry>
            <term>${constraint['constraint_name_dbk']}</term>
            <listitem><para>${constraint['constraint_dbk']}</para></listitem>
          </varlistentry>
      % if index + 1 == len(table['constraints']):
        </variablelist>
      % endif
    % endfor

    % for index, table_index in enumerate(table['indexes']):
      % if index == 0:
        <variablelist>
          <title>Indexes on ${table_index['table_dbk']}</title>
      % endif
          <varlistentry>
            <term>${table_index['index_name_dbk']}</term>
            <listitem><para>${table_index['index_definition_dbk']}</para></listitem>
          </varlistentry>
      % if index + 1 == len(table['indexes']):
        </variablelist>
      % endif
    % endfor

    % for index, fk_schema in enumerate(table['fk_schemas']):
      % if index == 0:
        <itemizedlist>
          <title>
            Tables referencing \
        % if 'number_of_schemas' in fk_schema:
${fk_schema['fk_schema_dbk'] | h}.\
        % endif
${fk_schema['fk_table_dbk'] | h} via Foreign Key Constraints
          </title>
      % endif
          <listitem>
            <para>
              <xref linkend="${fk_schema['fk_sgmlid']}"/>
            </para>
          </listitem>
      % if index + 1 == len(table['fk_schemas']):
        </itemizedlist>
      % endif
    % endfor

    % if table['view_definition']:
        <figure>
         <title>Definition of view ${table['table_dbk']}</title>
         <programlisting>${table['view_definition_dbk']}</programlisting>
        </figure>
    % endif
    % for index, permission in enumerate(table['permissions']):
      % if index == 0:
        <variablelist>
          <title>Permissions on \
        % if 'number_of_schemas' in permission:
${permission['schema'] | h}.\
        % endif
${permission['table_dbk']}</title>
      % endif
          <varlistentry>
            <term>${permission['user_dbk']}</term>
            <listitem>
              <para>
                <simplelist type="inline">
      % if 'select' in permission:
                  <member>Select</member>
      % endif
      % if 'insert' in permission:
                  <member>Insert</member>
      % endif
      % if 'update' in permission:
                  <member>Update</member>
      % endif
      % if 'delete' in permission:
                  <member>Delete</member>
      % endif
      % if 'rule' in permission:
                  <member>Rule</member>
      % endif
      % if 'references' in permission:
                  <member>References</member>
      % endif
      % if 'trigger' in permission:
                  <member>Trigger</member>
      % endif
                </simplelist>
              </para>
            </listitem>
          </varlistentry>
      % if index + 1 == len(table['permissions']):
        </variablelist>
      % endif
    % endfor

      </para>
    </section>
  % endfor

  % for function in schema['functions']:
<!-- Function ${function['function']} -->
    <section id="${function['function_sgmlid']}"
             xreflabel="${function['function_dbk']}">
      <title id="${function['function_sgmlid']}-title">
${function['function_dbk']}\
      </title>
      <titleabbrev id="${function['function_sgmlid']}-titleabbrev">
${function['function_dbk']}\
      </titleabbrev>

      <para>
       <segmentedlist>
        <title>Function Properties</title>
        <?dbhtml list-presentation="list"?>
        <segtitle>Language</segtitle>
        <segtitle>Return Type</segtitle>
        <seglistitem>
         <seg>${function['function_language'] | h}</seg>
         <seg>${function['function_returns'] | h}</seg>
        </seglistitem>
       </segmentedlist>
 
${function['function_comment_dbk']}\
        <programlisting>\
    % if function['function_source']:
${function['function_source'] | h}\
    % endif
</programlisting>
      </para>
    </section>
  % endfor
  </chapter>
% endfor
</book>

