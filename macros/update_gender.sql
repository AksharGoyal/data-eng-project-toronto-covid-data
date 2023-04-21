 {#
    This macro modifies Client Gender column 
#}

{% macro update_gender(gender) %}

    case {{ gender }}
        when 'OTHER' then 'UNKNOWN'
        when 'NOT LISTED, PLEASE SPECIFY' then 'UNKNOWN'
        else {{ gender }}
    end

{%- endmacro %}
              