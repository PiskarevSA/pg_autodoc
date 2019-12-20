<%
import re

def sgml_safe_id(string):
    # Lets use the keyword ARRAY in place of the square brackets
    # to prevent duplicating a non-array equivelent
    string = re.sub('\\[\\]', 'ARRAY-', string.lower())

    # Brackets, spaces, commas, underscores are not valid 'id' characters
    # replace with as few -'s as possible.
    string = re.sub('[ "\',)(_-]+', '-', string)

    # Don't want a - at the end either.  It looks silly.
    string = re.sub('-$', '', string)
    return string

def get_text(node_attr):
    result = str()
    if 'ERROR' in node_attr:
        result += '<b><font color="red">[ERROR: {}] </font></b>'.format(node_attr['ERROR'])
    node_name = (node_attr['SCHEMA'] + '.' if node_attr['SCHEMA'] is not None else '') + node_attr['OBJECT']
    url = None
    if 'LAYER_URL' in node_attr:
        url = node_attr['LAYER_URL']
    elif 'SERVICE_URL' in node_attr:
        url = node_attr['SERVICE_URL']
    elif node_attr['TYPE'] not in ('LAYER', 'SERVICE'):
        url = '#' + sgml_safe_id('.'.join((node_attr['SCHEMA'], node_attr['TYPE'], node_attr['OBJECT'])))
    result += node_attr['TYPE'] + '<br>'
    if url is not None:
        result += '<a href={}>{}</a>'.format(url, node_name)
    else:
        result += node_name
    return result

def tree_to_table(nodes, table, row=0, col=0):
    total_row_span = 0
    max_col = col
    for node_id in sorted(nodes.keys()):
        # set position
        while row >= len(table):
            table.append(list())
        while col >= len(table[row]):
            table[row].append(None)
        # set value
        cell_attr = table[row][col] = dict()
        cell_attr['TEXT'] = get_text(nodes[node_id]['ATTR'])
        # calc row span
        if 'CHILDS' in nodes[node_id]:
            node_row_span, node_max_col = tree_to_table(nodes[node_id]['CHILDS'], table, row, col+1)
            if max_col < node_max_col:
                max_col = node_max_col
        else:
            node_row_span = 1
        row += node_row_span
        cell_attr['ROW_SPAN'] = node_row_span
        total_row_span += node_row_span
    return total_row_span, max_col
%>\
<%def name="render_table(nodes)">\
<%
    table = list()
    total_row_span, max_col = tree_to_table(nodes, table)
    cols_count = max_col + 1 if total_row_span else 0
%>
<table>
    <caption><b>Зависимости слоёв и сервисов</b></caption>
    <thead>
        <tr>
            <th>Внешний объект</th>${' <th>Зависимости</th>' * (cols_count-1) if cols_count else ''}
        </tr>
    </thead>
    <tbody>
% for index, row in enumerate(table):
        <tr class=tr${index % 2}>
            ${' '.join(['<td' + (' rowspan="{}"'.format(col['ROW_SPAN']) if col['ROW_SPAN'] != 1 else '') + '>' +
            col['TEXT'] + '</td>' for col in row if col is not None])}\
            ${' <td class=inactive{} colspan="{}"/>'.format(index % 2, cols_count - len(row)) if cols_count > len(row) else ''}
        </tr>
% endfor
    </tbody>
</table>
</%def>\
${render_table(dependencies)}