{% macro week_trunc(thedate, offset=0) -%}
  DATE_TRUNC('week', {{ thedate }} + {{ offset }})::date - {{ offset }}
{%- endmacro %}