{% macro date_part(datepart, date) -%}
  extract({{ datepart }} from {{ date }})
{%- endmacro %}