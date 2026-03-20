{# 
  Calculates percent validity as:
  (observed / theoretical) * 100

  Returns 0 when theoretical = 0 to avoid division errors.

  IMPORTANT:
  Wrap inputs in parentheses so expressions like
  "available_days_ytd * 24" are evaluated correctly.
#}
{% macro calc_percentuale_validita(rilevati, teorici) %}
    round(
        case
            when ({{ teorici }}) = 0 then 0
            else (({{ rilevati }}) * 100.0 / ({{ teorici }}))
        end
    , 6)
{% endmacro %}

{#
  Returns indicator validity:
  - 'Si' if percent validity >= threshold
  - 'No' otherwise

  Default threshold is 75.

  IMPORTANT:
  Wrap inputs in parentheses so compound expressions are safe.
#}
{% macro calc_validita_indicatore(rilevati, teorici, threshold=75) %}
    case
        when ({{ teorici }}) = 0 then 'No'
        when ((({{ rilevati }}) * 100.0) / ({{ teorici }})) >= {{ threshold }} then 'Si'
        else 'No'
    end
{% endmacro %}

{#
  Standardizes the final output columns for one indicator row.
#}
{% macro generate_indicator_row(
    nome,
    rif_norm,
    cond_lim,
    note,
    valore,
    dati_teorici,
    valori_rilevati
) %}
    '{{ nome }}' as nome_indicatore,
    '{{ rif_norm }}' as riferimento_normativo,
    '{{ cond_lim }}' as condizione_sul_limite,
    {{ valore }} as valore,
    '{{ note }}' as note_di_lavoro,
    {{ dati_teorici }} as dati_teorici,
    {{ valori_rilevati }} as valori_rilevati,
    {{ calc_percentuale_validita(valori_rilevati, dati_teorici) }} as percentuale_validita,
    {{ calc_validita_indicatore(valori_rilevati, dati_teorici) }} as validita_indicatore
{% endmacro %}

{#
  Computes elapsed available days in the current year.
#}
{% macro available_year_elapsed_days(current_date_expr, first_available_date_expr) %}
    datediff(
        cast({{ current_date_expr }} as date),
        cast({{ first_available_date_expr }} as date)
    ) + 1
{% endmacro %}

{#
  Reusable window clause for year-to-date calculations.
#}
{% macro o3_ytd_window(date_col='data_day') %}
    partition by extract(year from {{ date_col }}), cod_ubic, cod_conf
    order by {{ date_col }}
    rows between unbounded preceding and current row
{% endmacro %}

{#
  Reusable YTD cumulative sum expression.
#}
{% macro ytd_sum(expr, date_col='data_day') %}
    sum({{ expr }}) over (
        {{ o3_ytd_window(date_col) }}
    )
{% endmacro %}