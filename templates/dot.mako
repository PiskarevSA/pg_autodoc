digraph g {
graph [
rankdir = "LR",
concentrate = true,
ratio = auto
];
node [
fontsize = "10",
shape = record
];
edge [
];
% for schema in schemas:
  % for table in schema['tables']:
    % if not table['view_definition']:
"\
      % if 'number_of_schemas' in table:
${table['schema_dot']}.\
      % endif
${table['table_dot']}" [shape = plaintext, label = < <TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0"> \
<TR ><TD PORT="ltcol0"> </TD> <TD bgcolor="grey90" border="1" COLSPAN="4"> \N </TD> <TD PORT="rtcol0"></TD></TR> \
      % for column in table['columns']:
 <TR><TD PORT="ltcol${column['column_number']}" ></TD><TD align="left" > ${column['column_dot']} </TD>\
<TD align="left" > ${column['column_type']} </TD><TD align="left" > \
        % for column_constraint in column['column_constraints']:
          % if 'column_primary_key' in column_constraint:
PK\
          % endif
        % endfor
 </TD><TD align="left" > \
        % for index, column_constraint in enumerate(column['column_constraints']):
          % if 'column_fk' in column_constraint:
            % if index == 0:
FK\
            % endif
          % endif
        % endfor
 </TD><TD align="left" PORT="rtcol${column['column_number']}"> </TD></TR>\
      % endfor
 </TABLE>> ];
    % endif
  % endfor
% endfor

% for fk_link in fk_links:
"\
  % if 'number_of_schemas' in fk_link:
${fk_link['handle0_schema']}.\
  % endif
${fk_link['handle0_name']}":rtcol${fk_link['handle0_connection']} -> "\
  % if 'number_of_schemas' in fk_link:
${fk_link['handle1_schema']}.\
  % endif
${fk_link['handle1_name']}":ltcol${fk_link['handle1_connection']} [label="${fk_link['fk_link_name_dot']}"];
% endfor
}
