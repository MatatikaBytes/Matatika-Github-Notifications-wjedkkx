{%- macro day_name(date, short=True) -%}
{%- set f = 'Dy' if short else 'Day' -%}
    (to_char({{ date }}, '{{ f }}'))
{%- endmacro %}