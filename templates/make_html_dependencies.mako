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
    result += node_attr['TYPE']
    if url is not None:
        result += '<a href={}>{}</a>'.format(url, node_name)
    else:
        result += node_name
    return result
%>\
<%def name="render_tree(nodes, level=0)">\
${'    ' * level}<ul>
% for node_id in sorted(nodes.keys()):
    ${'    ' * level}<li>${get_text(nodes[node_id]['ATTR'])}</li>
    % if 'CHILDS' in nodes[node_id]:
${render_tree(nodes[node_id]['CHILDS'], level+1)}\
    % endif
% endfor
${'    ' * level}</ul>
</%def>\
${render_tree(dependencies)}