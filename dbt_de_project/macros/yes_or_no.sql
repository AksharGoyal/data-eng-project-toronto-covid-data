{% test yes_or_no(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} not in ('Yes', 'No')

{% endtest %}