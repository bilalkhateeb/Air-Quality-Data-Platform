{% macro apply_8h_threshold_pair(value_expr, count_expr, value_alias, count_alias) %}
    case
        when {{ count_expr }} >= 6 then {{ value_expr }}
    end as {{ value_alias }},

    case
        when {{ count_expr }} >= 6 then {{ count_expr }}
    end as {{ count_alias }}
{% endmacro %}