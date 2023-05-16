{{ config(
    materialized='table'
    , partition_by={
      "field": "day_date"
      , "data_type": "date"
    }
) }}

with
    dates as (
        select distinct
            day_date
            , dayofweek
            , day_name
            , dayofmonth
            , dayofyear
            , month
            , month_name
            , month_name_initials
            , quarter
            , year
            , day_month
            , month_year
        from {{ ref('stg_datetimes') }}
    )
select *
from dates
