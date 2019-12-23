<?xml version="1.0" encoding="UTF-8"?>
<dia:diagram xmlns:dia="http://www.lysator.liu.se/~alla/dia/">
  <dia:layer name="Background" visible="true">
% for schema in schemas:
  % if 'number_of_schemas' in schema:
    <dia:group>
  % endif
  % for table in schema['tables']:
    <dia:object type="UML - Class" version="0" id="O${table['object_id'] | h}">
      <dia:attribute name="obj_pos">
        <dia:point val="0,0"/>
      </dia:attribute>
      <dia:attribute name="obj_bb">
        <dia:rectangle val="-0.05,-0.05;16.4,6.65"/>
      </dia:attribute>
      <dia:attribute name="elem_corner">
        <dia:point val="0,0"/>
      </dia:attribute>
      <dia:attribute name="elem_width">
        <dia:real val="16.350000000000001"/>
      </dia:attribute>
      <dia:attribute name="elem_height">
        <dia:real val="6.6000000000000005"/>
      </dia:attribute>
      <dia:attribute name="name">
        <dia:string>#${table['table'] | h}#</dia:string>
      </dia:attribute>
    % if 'number_of_schemas' in table:
      <dia:attribute name="stereotype">
        <dia:string>#${table['schema'] | h}#</dia:string>
      </dia:attribute>
    % endif
      <dia:attribute name="comment">
        <dia:string>#${table['table_comment_dia'] | h}#</dia:string>
      </dia:attribute>
      <dia:attribute name="abstract">
        <dia:boolean val="false"/>
      </dia:attribute>
      <dia:attribute name="suppress_attributes">
        <dia:boolean val="false"/>
      </dia:attribute>
      <dia:attribute name="suppress_operations">
        <dia:boolean val="false"/>
      </dia:attribute>
      <dia:attribute name="visible_attributes">
        <dia:boolean val="true"/>
      </dia:attribute>
      <dia:attribute name="visible_comments">
        <dia:boolean val="true"/>
      </dia:attribute>
      <dia:attribute name="wrap_operations">
        <dia:boolean val="false"/>
      </dia:attribute>
      <dia:attribute name="wrap_after_char">
        <dia:int val="40"/>
      </dia:attribute>
      <dia:attribute name="line_color">
        <dia:color val="#000000"/>
      </dia:attribute>
      <dia:attribute name="fill_color">
        <dia:color val="#ffffff"/>
      </dia:attribute>
      <dia:attribute name="text_color">
        <dia:color val="#000000"/>
      </dia:attribute>
      <dia:attribute name="normal_font">
        <dia:font family="monospace" style="0" name="Courier"/>
      </dia:attribute>
      <dia:attribute name="abstract_font">
        <dia:font family="monospace" style="88" name="Courier"/>
      </dia:attribute>
      <dia:attribute name="polymorphic_font">
        <dia:font family="monospace" style="8" name="Courier"/>
      </dia:attribute>
      <dia:attribute name="classname_font">
        <dia:font family="sans" style="80" name="Helvetica"/>
      </dia:attribute>
      <dia:attribute name="abstract_classname_font">
        <dia:font family="sans" style="88" name="Helvetica"/>
      </dia:attribute>
      <dia:attribute name="comment_font">
        <dia:font family="sans" style="8" name="Helvetica"/>
      </dia:attribute>
      <dia:attribute name="font_height">
        <dia:real val="0.80000000000000004"/>
      </dia:attribute>
      <dia:attribute name="polymorphic_font_height">
        <dia:real val="0.80000000000000004"/>
      </dia:attribute>
      <dia:attribute name="abstract_font_height">
        <dia:real val="0.80000000000000004"/>
      </dia:attribute>
      <dia:attribute name="classname_font_height">
        <dia:real val="1"/>
      </dia:attribute>
      <dia:attribute name="abstract_classname_font_height">
        <dia:real val="1"/>
      </dia:attribute>
      <dia:attribute name="comment_font_height">
        <dia:real val="1"/>
      </dia:attribute>
      <dia:attribute name="attributes">
    % for column in table['columns']:
        <dia:composite type="umlattribute">
          <dia:attribute name="name">
            <dia:string>#\
      % if 'column_primary_key' in column:
PK\
      % else:
  \
      % endif
${column['column'] | h}#</dia:string>
          </dia:attribute>
          <dia:attribute name="type">
            <dia:string>#${column['column_type'] | h}#</dia:string>
          </dia:attribute>
          <dia:attribute name="value">
      % if column['column_default_short']:
            <dia:string>#${column['column_default_short'] | h}#</dia:string>
      % else:
            <dia:string/>
      % endif
          </dia:attribute>
          <dia:attribute name="visibility">
            <dia:enum val="3"/>
          </dia:attribute>
          <dia:attribute name="abstract">
            <dia:boolean val="false"/>
          </dia:attribute>
          <dia:attribute name="class_scope">
            <dia:boolean val="false"/>
          </dia:attribute>
        </dia:composite>
    % endfor
      </dia:attribute>
    % if table['constraints']:
      <dia:attribute name="visible_operations">
        <dia:boolean val="true"/>
      </dia:attribute>
      <dia:attribute name="operations">
      % for constraint in table['constraints']:
        <dia:composite type="umloperation">
          <dia:attribute name="name">
            <dia:string>#${constraint['constraint_name'] | h}#</dia:string>
          </dia:attribute>
          <dia:attribute name="visibility">
            <dia:enum val="3"/>
          </dia:attribute>
          <dia:attribute name="abstract">
            <dia:boolean val="false"/>
          </dia:attribute>
          <dia:attribute name="class_scope">
            <dia:boolean val="false"/>
          </dia:attribute>
          <dia:attribute name="parameters">
            <dia:composite type="umlparameter">
              <dia:attribute name="name">
                <dia:string>#${constraint['constraint_short'] | h}#</dia:string>
              </dia:attribute>
              <dia:attribute name="type">
                <dia:string>##</dia:string>
              </dia:attribute>
              <dia:attribute name="value">
                <dia:string/>
              </dia:attribute>
              <dia:attribute name="kind">
                <dia:enum val="0"/>
              </dia:attribute>
            </dia:composite>
          </dia:attribute>
        </dia:composite>
      % endfor
      </dia:attribute>
    % else:
      <dia:attribute name="visible_operations">
        <dia:boolean val="false"/>
      </dia:attribute>
      <dia:attribute name="operations"/>
    % endif
      <dia:attribute name="template">
        <dia:boolean val="false"/>
      </dia:attribute>
      <dia:attribute name="templates"/>
    </dia:object>
  % endfor
  % if 'number_of_schemas' in schema:
    </dia:group>
  % endif
% endfor
% for fk_link in fk_links:
    <dia:object type="UML - Constraint" version="0" id="O${fk_link['object_id'] | h}">
      <dia:attribute name="obj_pos">
        <dia:point val="0,3.5"/>
      </dia:attribute>
      <dia:attribute name="obj_bb">
        <dia:rectangle val="-0.0515705,2.29861;25.1127,3.55157"/>
      </dia:attribute>
      <dia:attribute name="conn_endpoints">
        <dia:point val="0,3.5"/>
        <dia:point val="25.05,2.7"/>
      </dia:attribute>
      <dia:attribute name="constraint">
        <dia:string>#${fk_link['fk_link_name'] | h}#</dia:string>
      </dia:attribute>
      <dia:attribute name="text_pos">
        <dia:point val="12.525,3.1"/>
      </dia:attribute>
      <dia:attribute name="line_colour">
        <dia:color val="#000000"/>
      </dia:attribute>
      <dia:connections>
        <dia:connection handle="0" to="O${fk_link['handle0_to'] | h}" connection="${fk_link['handle0_connection_dia'] | h}"/>
        <dia:connection handle="1" to="O${fk_link['handle1_to'] | h}" connection="${fk_link['handle1_connection_dia'] | h}"/>
      </dia:connections>
    </dia:object>
% endfor
  </dia:layer>
</dia:diagram>
