{{
    config(
        pre_hook="""
            alter external table {{ source('mock_provider', 'equity_index_future__tick_data') }} refresh;
        """,
    )
}}

SELECT
  internal_id::varchar                                                AS index_internal_id,
  data_provider::varchar                                              AS data_provider,
  value:"Instrument"::varchar                                         AS contract_symbol,
  value:"Price"::float                                                AS price,
  TO_TIMESTAMP(value:"Time"::varchar, 'YYYYMMDDHHMISSFF3')::timestamp AS trading_datetime,
  value:"Volume"::int                                                 AS volume,
  CURRENT_TIMESTAMP()::timestamp                                      AS ingested_at
FROM {{ source('mock_provider', 'equity_index_future__tick_data') }}
