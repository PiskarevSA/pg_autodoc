digraph g {
node [ fontsize = "10", shape = record ];
edge [];
% for schema in schemas:
  % for table in schema['tables']:
    % if not table['view_definition']:
"\
      % if 'number_of_schemas' in table:
${table['schema_dot']}.\
      % endif
${table['table_dot']}" [shape = record, label = "{<col0> \N| \
      % for column in table['columns']:
${column['column_dot']}:  ${column['column_type']}\l\
      % endfor
}" ];
    % endif
  % endfor
% endfor
% for fk_link in fk_links:
"\
  % if 'number_of_schemas' in fk_link:
${fk_link['handle0_schema']}.\
  % endif
${fk_link['handle0_name']}" -> "\
  % if 'number_of_schemas' in fk_link:
${fk_link['handle1_schema']}.\
  % endif
${fk_link['handle1_name']}" [label="${fk_link['fk_link_name_dot']}"];
% endfor
}


