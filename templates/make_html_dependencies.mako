<% def sgml_safe_id(string):
    import re
    # Lets use the keyword ARRAY in place of the square brackets
    # to prevent duplicating a non-array equivelent
    string = re.sub('\\[\\]', 'ARRAY-', string.lower())

    # Brackets, spaces, commas, underscores are not valid 'id' characters
    # replace with as few -'s as possible.
    string = re.sub('[ "\',)(_-]+', '-', string)

    # Don't want a - at the end either.  It looks silly.
    string = re.sub('-$', '', string)

    return string
%>\
<%def name="render_tree(nodes, level=0)">\
${'    ' * level}<ul>
% for node_id in sorted(nodes.keys()):
<% attr = nodes[node_id]['ATTR'] %>\
${'    ' * level}    <li>\
    % if 'ERROR' in attr:
<b><font color="red">${'[ERROR: {}] '.format(attr['ERROR'])}</font></b>\
    % endif
<%
    node_name = (attr['SCHEMA'] + '.' if attr['SCHEMA'] is not None else '') + attr['OBJECT']
    url = None
    if 'LAYER_URL' in attr:
        url = attr['LAYER_URL']
    elif 'SERVICE_URL' in attr:
        url = attr['SERVICE_URL']
    elif attr['TYPE'] not in ('LAYER', 'SERVICE'):
        url = '#' + sgml_safe_id('.'.join((attr['SCHEMA'], attr['TYPE'], attr['OBJECT'])))
%>\
${attr['TYPE']} \
% if url is not None:
<a href=${url}>${node_name}</a>\
% else:
${node_name}\
% endif
</li>
    % if 'CHILDS' in nodes[node_id]:
${render_tree(nodes[node_id]['CHILDS'], level+1)}\
    % endif
% endfor
${'    ' * level}</ul>
</%def>\
${render_tree(dependencies)}