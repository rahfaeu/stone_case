{{ config(
    materialized='table'
    , partition_by={
      "field": "date_time"
      , "data_type": "timestamp"
      , "granularity": "month"
    }
) }}

with
    dates as (
        select distinct
            date_time
            , day_date
            , time
            , hour
            , day_shift
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