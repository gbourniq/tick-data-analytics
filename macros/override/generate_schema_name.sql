/*
    This macro is needed to override dbt's default schema generation behavior.
    It prevents the issue where dbt would automatically append the target schema
    to custom schema names, which could lead to unintended schema structures.
    This version ensures that custom schema names are used as-is, without modification.
*/

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {% else %}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
