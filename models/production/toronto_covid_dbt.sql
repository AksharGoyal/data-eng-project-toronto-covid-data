{{ config(materialized='table',
    partition_by={
        "field":"Episode_Date",
        "data_type":"date",
        "granularity":"day"
    },
    cluster_by='Assigned_ID'
    )
}}


with toronto_data as (
    select * from {{ source('production', 'ext_partitioned_clustered_covid_toronto_data') }} order by Episode_Date
)
select 

    cast(assigned_id as integer) as Assigned_ID,
    cast({{ age_group('Age_Group')}} as string) as Age_Group,
    cast({{update_gender('Client_gender')}} as string) as Client_Gender,
    cast(neighbourhood_name as string) as Neighbourhood,
    cast(FSA as string) as Postal_District,
    cast(outbreak_associated as string) as Outbreak_Associated,
    cast(classification as string) as Classification,
    cast(source_of_infection as string) as Source,
    cast(episode_date as date) as Episode_Date,
    cast(reported_date as date) as Reported_Date,
    cast(date_diff(date(reported_date), date(episode_date), day) as integer) as Delay_In_Reporting,
    cast(Outcome as string) as Outcome,
    cast(ever_hospitalized as string) as Ever_Hospitalized,
    cast(ever_in_ICU as string) as Ever_In_ICU,
    cast(ever_intubated as string) as Ever_Intubated 
    from toronto_data

-- dbt build --m <model.sql> --var 'test: true'
{% if var('test', default=false) %}

  limit 100

{% endif %}