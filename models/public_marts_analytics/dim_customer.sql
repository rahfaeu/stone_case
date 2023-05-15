{{ config (
    materialized = 'table'
    , cluster_by = [
        'transaction_user_id'
        , 'transaction_user_state'
        , 'transaction_user_city'
    ]
) }}

with
    transform_data as (
        select
            transaction_user_id
            , transaction_user_state
            , transaction_user_city
            , row_number() over(
                partition by
                    transaction_user_id
                    , transaction_user_state
                    , transaction_user_city
            ) as customer_rn
        from {{ ref('stg_user_transactions') }}
        qualify customer_rn = 1
    )
    /* In case of redshift use, the qualify() function not exists.
       So, use following cte to get unique customers:
     
    , redshit_uses(
        select *
        from transform_data
        where customer_rn = 1
    )
    */
    

select * except(customer_rn)
from transform_data
-- from redshit_uses