<%def name="permission_cell(permissions, aspect)">\
% if aspect in permissions:
<td style="text-align:center">&diams;</td>\
% else:
<td></td>\
% endif
</%def>

<!DOCTYPE html>

<html lang="ru">
<head>
  <title>Index for ${database | h}</title>
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
  <style>
  BODY {
    color:	#000000;
    background-color: #FFFFFF;
    font-family: Helvetica, sans-serif;
  }

  P {
    margin-top: 5px;
    margin-bottom: 5px;
  }

  P.w3ref {
    font-size: 8pt;
    font-style: italic;
    text-align: right;
  }

  P.detail {
    font-size: 10pt;
  }

  .error {
    color: #FFFFFF;
    background-color: #FF0000;
  }

  H1, H2, H3, H4, H5, H6 {
  }

  OL {
    list-style-type: upper-alpha;
  }

  UL.topic {
    list-style-type: upper-alpha;
  }

  LI.topic {
    font-weight : bold;
  }

  HR {
    color: #00FF00;
    background-color: #808080;
  }

  TABLE {
    border-width: medium;
    padding: 3px;
    background-color: #000000;
    width: 90%;
  }

  CAPTION {
    text-transform: capitalize;
    font-weight : bold;
    font-size: 14pt;
  }

  TH {
    padding: 3px;
    color: #FFFFFF;
    background-color: #000000;
    text-align: left;
  }

  TR {
    color: #000000;
    background-color: #FFFFFF;
    vertical-align: top;
  }

  TR.tr0 {
    background-color: #F0F0F0;
  }

  TR.tr1 {
    background-color: #D8D8D8;
  }

  TD {
    padding: 3px;
    font-size: 12pt;
  }

  TD.inactive0 {
    background-color: #B8B8B8;
  }

  TD.inactive1 {
    background-color: #B0B0B0;
  }

  TD.col0 {
    font-weight : bold;
    width: 20%;
  }

  TD.col1 {
    font-style: italic;
    width: 15%;
  }

  TD.col2 {
    font-size: 12px;
  }

  </style>
  <link rel="stylesheet" type="text/css" media="all" href="all.css">
  <link rel="stylesheet" type="text/css" media="screen" href="screen.css">
  <link rel="stylesheet" type="text/css" media="print" href="print.css">
</head>
<body>

<!-- Primary Index -->
<p>${database_comment_html}<br><br>Dumped on ${dumped_on | h}</p>
<h1><a id="index">Index of database - ${database | h}</a></h1>
<ul>
% for schema in schemas:
  <li><a id="${schema['schema_sgmlid'] | u}">${schema['schema'] | h}</a>
    <details>
    <summary>Содержимое схемы</summary>
    <ul>
    % for table in schema['tables']:
      <li><a href="#${table['table_sgmlid'] | u}">${table['table'] | h}</a></li>
    % endfor
    % for function in schema['functions']:
      <li><a href="#${function['function_sgmlid'] | u}">${function['function'] | h}</a></li>
    % endfor
    </ul>
    </details>
  </li>
% endfor
</ul>

<!-- Dependencies table -->
% if dependencies:
<h1>Layers and services dependencies</h1>
${dependencies}
% endif

<!-- Schema Creation -->
% for schema in schemas:
<!-- Schema ${schema['schema'] | h} -->

% if 'number_of_schemas' in schema:
<hr>
<h1>Schema ${schema['schema'] | h}</h1>
% if schema['schema_comment_html']:
<p class="schema_comment_html">${schema['schema_comment_html']}</p>
% endif
% endif

% for table in schema['tables']:
<hr>
<h2>${table['table_type']}  \
  % if 'number_of_schemas' in table:
<a href="#${table['schema_sgmlid'] | u}">${table['schema'] | h}</a>.\
  % endif
<a id="${table['table_sgmlid'] | u}">${table['table'] | h}</a>
</h2>
% if table['table_comment_html']:
<p class="table_comment_html">${table['table_comment_html']}</p>
% endif


<table class="schema" style="width:100%; border-spacing: 0;">
  <caption>\
% if 'number_of_schemas' in table:
${table['schema'] | h}.\
% endif
${table['table'] | h} Structure</caption>
  <tr>
    <th>F-Key</th>
    <th>Name</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  % for index, column in enumerate(table['columns']):
  <tr class="tr${index % 2}">
    <td>
      % for column_constraint in column['column_constraints']:
      % if 'column_fk' in column_constraint:
      <a href="#${column_constraint['column_fk_sgmlid'] | u}">\
      % if 'number_of_schemas' in column_constraint:
${column_constraint['column_fk_schema'] | h}.\
      % endif
${column_constraint['column_fk_table'] | h}.\
${column_constraint['column_fk_column'] | h}\
      % if column_constraint['column_fk_keygroup']:
#${column_constraint['column_fk_keygroup']}\
      % endif
</a>
      % endif
      % endfor
    </td>
    <td>${column['column'] | h}</td>
    <td>${column['column_type'] | h}</td>
    <td>
      <i> \
        % for column_constraint in column['column_constraints']:
          % if 'column_primary_key' in column_constraint:
PRIMARY KEY \
          % endif
          % if 'column_unique' in column_constraint:
UNIQUE \
            % if column_constraint['column_unique_keygroup']:
#${column_constraint['column_unique_keygroup']}
            % endif
          % endif
        % endfor
        % if column.get('column_constraint_notnull'):
NOT NULL \
        % endif
        % if column['column_default']:
DEFAULT ${column['column_default'] | h} \
        % endif
</i>
      % if column['column_comment_html']:
      <br><br>${column['column_comment_html']}
      % endif
    </td>
  </tr>
  % endfor
</table>

<!-- Inherits -->
% if table['inherits']:
<p>Table \
% if 'number_of_schemas' in table:
${table['schema'] | h}.\
% endif
${table['table'] | h} Inherits
  % for inherit in table['inherits']:
  <a href="#${inherit['parent_sgmlid'] | u}">\
    % if 'number_of_schemas' in inherit:
${inherit['parent_schema'] | h}.\
    % endif
${inherit['parent_table'] | h}</a>,
  % endfor
</p>
% endif

<!-- Statistics -->
% if table['stats_enabled']:
<p>&nbsp;</p>
<table style="width:100%; border-spacing: 0;">
  <caption>Statistics</caption>
  <tr>
    <th>Total Space (disk usage)</th>
    <th>Tuple Count</th>
    <th>Active Space</th>
    <th>Dead Space</th>
    <th>Free Space</th>
  </tr>
  <tr class="tr0">
    <td>${table['stats_table_bytes'] | h}</td>
    <td>${table['stats_tuple_count'] | h}</td>
    <td>${table['stats_tuple_bytes'] | h}</td>
    <td>${table['stats_dead_bytes'] | h}</td>
    <td>${table['stats_free_bytes'] | h}</td>
  </tr>
</table>
% endif

<!-- Constraint List -->
% if table['constraints']:
<p>&nbsp;</p>
<table class="constraints" style="width:100%; border-spacing: 0;">
  <caption>\
  % if 'number_of_schemas' in table:
${table['schema'] | h}.\
  % endif
${table['table'] | h} Constraints</caption>
  <tr>
    <th>Name</th>
    <th>Constraint</th>
  </tr>
  % for index, constraint in enumerate(table['constraints']):
  <tr class="tr${index % 2}">
    <td>${constraint['constraint_name'] | h}</td>
    <td>${constraint['constraint'] | h}</td>
  </tr>
  % endfor
</table>
% endif

<!-- Foreign Key Discovery -->
% if table['fk_schemas']:
<div class="fk_schemas">
  <p>Tables referencing this one via Foreign Key Constraints:</p>
% for fk_schema in table['fk_schemas']:
  <ul>
    <li><a href="#${fk_schema['fk_sgmlid'] | u}">\
    % if 'number_of_schemas' in fk_schema:
${fk_schema['fk_schema'] | h}.\
    % endif
${fk_schema['fk_table'] | h}</a></li>
  </ul>
% endfor
</div>
% endif
<ul class="indexes">
<!-- Indexes -->
% for index in table['indexes']:
    <li><b>${index['index_name']}</b> ${index['index_definition']}</li>
% endfor
</ul>
<!-- View Definition -->
% if table['view_definition']:
<details>
<summary>Исходный код представления</summary>
<pre>${table['view_definition'] | h}</pre>
</details>
% endif

<!-- List off permissions -->
% if table['permissions']:
<p>&nbsp;</p>
<table style="width:100%; border-spacing: 0;">
  <caption>Permissions which apply to \
% if 'number_of_schemas' in table:
${table['schema'] | h}.\
% endif
${table['table'] | h}</caption>
  <tr>
    <th>User</th>
    <th style="text-align:center">Select</th>
    <th style="text-align:center">Insert</th>
    <th style="text-align:center">Update</th>
    <th style="text-align:center">Delete</th>
    <th style="text-align:center">Reference</th>
    <th style="text-align:center">Rule</th>
    <th style="text-align:center">Trigger</th>
  </tr>
  % for index, permission in enumerate(table['permissions']):
  <tr class="tr${index % 2}">
    <td>${permission['user'] | h}</td>
    ${permission_cell(permission, 'select')}
    ${permission_cell(permission, 'insert')}
    ${permission_cell(permission, 'update')}
    ${permission_cell(permission, 'delete')}
    ${permission_cell(permission, 'references')}
    ${permission_cell(permission, 'rule')}
    ${permission_cell(permission, 'trigger')}
  </tr>
  % endfor
</table>
% endif
<p>
  <a href="#index">Index</a> -
  <a href="#${table['schema_sgmlid'] | u}">Schema ${table['schema'] | h}</a>
</p>
% endfor

<!-- We've gone through the table structure, now lets take a look at user functions -->
% for function in schema['functions']:
<hr>
<h2>Function:
<a href="#${function['schema_sgmlid'] | u}">\
  % if 'number_of_schemas' in function:
${function['schema'] | h}</a>.\
  % endif
<a id="${function['function_sgmlid'] | u}">${function['function'] | h}</a>
</h2>
<h3>Returns: ${function['function_returns'] | h}</h3>
<h3>Language: ${function['function_language'] | h}</h3>
% if function['function_comment_html']:
<p>${function['function_comment_html']}</p>
% endif
<details>
<summary>Исходный код функции</summary>
<pre>\
% if function['function_source']:
${function['function_source'] | h}\
% endif
</pre>
</details>
% endfor
% endfor
<p class="w3ref">Generated by <a href="http://github.com/cbbrowne/autodoc/">PostgreSQL Autodoc</a></p>
<p class="w3ref"><a href="http://validator.w3.org/check?uri=referer">W3C HTML 5.2 Strict</a></p>
</body></html>
