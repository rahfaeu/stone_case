{{ config (
    materialized = 'table'
    , cluster_by = [
        'transaction_capture_method'
        , 'transaction_card_brand'
        , 'transaction_payment_method'
        , 'transaction_status'
    ]
) }}

with
    raw_data as(
        select
            case
                when lower(transaction_capture_method) = '<null>' then null
                else initcap(transaction_capture_method)
            end as transaction_capture_method
            , case
                when lower(transaction_card_brand) = '<null>' then null
                else initcap(transaction_card_brand)
            end as transaction_card_brand
            , case
                when lower(transaction_payment_method) = '<null>' then null
                else initcap(transaction_payment_method)
            end as transaction_payment_method
            , case
                when lower(transaction_status) = '<null>' then null
                else initcap(transaction_status)
            end as transaction_status
        from {{ ref('stg_user_transactions') }}
        group by
            transaction_capture_method
            , transaction_card_brand
            , transaction_payment_method
            , transaction_status
    )
  , adding_sk as (
    select 
        {{ numeric_surrogate_key([
            'transaction_capture_method'
            , 'transaction_card_brand'
            , 'transaction_payment_method'
            , 'transaction_status'
        ]) }} as transaction_infos_sk
        ,  transaction_capture_method
            , transaction_card_brand
            , transaction_payment_method
            , transaction_status
    from raw_data
  )
select *
from adding_sk