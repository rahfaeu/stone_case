{{ config (
    materialized = 'incremental'
    , incremental_strategy = 'insert_overwrite'
    , partitions = var('partitions_to_replace_last_two_days')
    , partition_by = {
        'field': 'transaction_date'
        , 'data_type': 'date'
    }
    , cluster_by = [
        'transaction_id'
        , 'transaction_user_sk'
        , 'transaction_infos_sk'
    ]
) }}

with
    raw_data as(
        select
            transaction_id
            , transaction_at
            , transaction_date
            , transaction_value
            , transaction_user_id
            , transaction_user_state
            , transaction_user_city
            , case
                when transaction_capture_method = '<null>' then null
                else initcap(transaction_capture_method)
            end as transaction_capture_method
            , case
                when transaction_card_brand = '<null>' then null
                else initcap(transaction_card_brand)
            end as transaction_card_brand
            , case
                when transaction_payment_method = '<null>' then null
                else initcap(transaction_payment_method)
            end as transaction_payment_method
            , case
                when transaction_status = '<null>' then null
                else initcap(transaction_status)
            end as transaction_status
        from {{ ref('stg_user_transactions') }}
        {% if is_incremental() %}
            where cast(datetime(data_e_hora_da_transacao, 'America/Sao_Paulo') as date) in ({{ var('partitions_to_replace_short') | join(',') }})
        {% endif %}
    )
    , transform as (
        select
            transaction_id
            , {{ numeric_surrogate_key([
                'transaction_user_id'
                , 'transaction_user_state'
                , 'transaction_user_city'
            ]) }} as transaction_user_sk
            , {{ numeric_surrogate_key([
                'transaction_capture_method'
                , 'transaction_card_brand'
                , 'transaction_payment_method'
                , 'transaction_status'
            ]) }} as transaction_infos_sk
            , transaction_at
            , transaction_date
            , transaction_value
        from raw_data
    )
select *
from transform

