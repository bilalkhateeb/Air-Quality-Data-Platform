{# 
  Calculates percent validity as:
  (observed / theoretical) * 100
  Returns 0 when theoretical = 0 to avoid division errors.
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
#}
{% macro calc_validita_indicatore(rilevati, teorici, threshold=75) %}
    case
        when ({{ teorici }}) = 0 then 'No'
        when ((({{ rilevati }}) * 100.0) / ({{ teorici }})) >= {{ threshold }} then 'Si'
        else 'No'
    end
{% endmacro %}

{#
  Standardizes the final output columns for one daily indicator row.
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
  Standardizes the final output columns for one annual indicator row.
#}
{% macro generate_annual_indicator_row(
    nome,
    rif_norm,
    cond_lim,
    livello_critico,
    valore,
    dati_teorici,
    valori_rilevati
) %}
    '{{ nome }}' as nome_indicatore,
    '{{ rif_norm }}' as valore_limite,
    '{{ cond_lim }}' as condizione_sul_limite,
    '{{ livello_critico }}' as livello_critico,
    {{ valore }} as valore,
    {{ dati_teorici }} as dati_teorici,
    {{ valori_rilevati }} as valori_rilevati,
    {{ calc_percentuale_validita(valori_rilevati, dati_teorici) }} as percentuale_validita,
    {{ calc_validita_indicatore(valori_rilevati, dati_teorici) }} as validita_indicatore
{% endmacro %}

{#
  For each row, calculate how many days have passed 
  since the first available loaded day in that same year, up to the current day.
#}
{% macro available_year_elapsed_days(current_date_expr, first_available_date_expr) %}
    datediff(
        cast({{ current_date_expr }} as date),
        cast({{ first_available_date_expr }} as date)
    ) + 1
{% endmacro %}

{#
  Returns the number of available hourly theoretical observations
  in the loaded portion of a given year for annual indicators.
#}
{% macro available_year_hourly_theoretical(first_available_date_expr, last_available_date_expr) %}
    (
        datediff(
            cast({{ last_available_date_expr }} as date),
            cast({{ first_available_date_expr }} as date)
        ) + 1
    ) * 24
{% endmacro %}

{# Reusable YTD cumulative sum expression. Up to this day, how many have we accumulated since the beginning of the year? #}
{% macro ytd_sum(expr, date_col='data_day') %}
    sum({{ expr }}) over (
        {{ ytd_window(date_col) }}
    )
{% endmacro %}

{# For each station/configuration and each year, it keeps adding values from the start of the year up to the current day. #}
{% macro ytd_window(date_col='data_day') %}
    partition by extract(year from {{ date_col }}), cod_ubic, cod_conf
    order by {{ date_col }}
    rows between unbounded preceding and current row
{% endmacro %}


{# Builds a rolling 3-hour sum of hourly exceedance flags within the same day.
  The partition includes data_day so the sequence does not cross midnight.
  coalesce acts as a safety net. If there is no previous hour, it substitutes a 0 #}
{% macro rolling_3h_threshold_sum(flag_expr) %}
(
    {{ flag_expr }}
    + coalesce(
        lag({{ flag_expr }}, 1) over (
            partition by cod_ubic, cod_conf, data_day
            order by data_da
        ),
        0
    )
    + coalesce(
        lag({{ flag_expr }}, 2) over (
            partition by cod_ubic, cod_conf, data_day
            order by data_da
        ),
        0
    )
)
{% endmacro %}