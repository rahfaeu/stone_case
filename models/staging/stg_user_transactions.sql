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
        , 'transaction_user_id'
        , 'transaction_status'
    ]
) }}

with
    /* Applying casting and defining columns names following convention naming */
    transform_data as (
        select
            {{ numeric_surrogate_key([
                'codigo_da_transacao'
                , 'codigo_do_usuario'
                , 'estado_do_usuario'
                , 'cidade_do_usuario'
            ]) }} as transaction_sk
            , cast(codigo_da_transacao as int64) as transaction_id
            , datetime_trunc(datetime(data_e_hora_da_transacao, 'America/Sao_Paulo'), minute) as transaction_at
            , cast(datetime(data_e_hora_da_transacao, 'America/Sao_Paulo') as date) as transaction_date
            , cast(metodo_de_captura as string) as transaction_capture_method
            , cast(bandeira_do_cartao as string) as transaction_card_brand
            , cast(metodo_de_pagamento as string) as transaction_payment_method
            , cast(estado_da_transacao as string) as transaction_status
            , cast(valor_da_transacao as float64) as transaction_value
            , cast(codigo_do_usuario as int64) as transaction_user_id
            , cast(estado_do_usuario as string) as transaction_user_state
            , cast(cidade_do_usuario as string) as transaction_user_city
        from {{ source('stone_case', 'transacoes_usuarios') }}
        {% if is_incremental() %}
            where cast(datetime(data_e_hora_da_transacao, 'America/Sao_Paulo') as date) in ({{ var('partitions_to_replace_short') | join(',') }})
        {% endif %}
    )
select *
from transform_data