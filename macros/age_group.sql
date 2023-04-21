 {#
    This macro modifies Age group column 
#}

{% macro age_group(age) %}

    case {{ age }}
        when '19 and younger' then '19-'
        when '20 to 29 Years' then '20-29'
        when '30 to 39 Years' then '30-39'
        when '40 to 49 Years' then '40-49'
        when '50 to 59 Years' then '50-59'
        when '60 to 69 Years' then '60-69'
        when '70 to 79 Years' then '70-79'
        when '80 to 89 Years' then '80-89'
        when '90 and older' then '90+'
        else {{ age }}
    end

{%- endmacro %}

              