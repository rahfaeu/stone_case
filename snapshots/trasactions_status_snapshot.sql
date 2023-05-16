
{% snapshot trasactions_status_snapshot %}

    {{
        config(
          target_schema = 'snapshots'
          , strategy = 'check'
          , unique_key = 'transaction_sk'
          , check_cols = ['transaction_status']
        )
    }}

    select
        transaction_sk
        , transaction_id
        , transaction_at
        , transaction_status
    from {{ ref('stg_user_transactions') }}

{% endsnapshot %}