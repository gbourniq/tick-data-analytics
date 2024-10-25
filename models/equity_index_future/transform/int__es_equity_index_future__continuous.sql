/* Extracts the contract month and year from the contract symbol. */
WITH parsed AS (
  SELECT
    contract_symbol,
    SUBSTRING(contract_symbol, 3, 1) AS contract_month,
    CASE
      WHEN CAST(SUBSTRING(contract_symbol, 4, 2) AS int) < 80 THEN 2000 + CAST(SUBSTRING(contract_symbol, 4, 2) AS int)
      ELSE 1900 + CAST(SUBSTRING(contract_symbol, 4, 2) AS int)
    END                              AS contract_year,
    price,
    trading_datetime,
    volume
  FROM {{ ref('stg__mock_provider__equity_index_future__tick_data') }}
  WHERE index_internal_id = 'ES_INDEX_FUTURES'
),

/* Orders contracts and assigns row numbers */
/* The row_num field is used for the cumulative adjustments since we have some identical trading_datetime */
ordered AS (
  SELECT
    contract_symbol,
    CASE contract_month
      WHEN 'H' THEN 1
      WHEN 'M' THEN 2
      WHEN 'U' THEN 3
      WHEN 'Z' THEN 4
    END                                                                        AS month_order,
    contract_month,
    contract_year,
    price,
    trading_datetime,
    volume,
    ROW_NUMBER() OVER (PARTITION BY contract_symbol ORDER BY trading_datetime) AS row_num
  FROM parsed
),

/* Identifies roll dates */
roll_adjustments AS (
  SELECT
    contract_symbol,
    contract_year,
    contract_month,
    month_order,
    trading_datetime,
    price,
    volume,
    row_num,
    LEAD(contract_symbol) OVER (ORDER BY contract_year, month_order, row_num) AS next_contract_symbol,
    LEAD(price) OVER (ORDER BY contract_year, month_order, row_num)           AS next_price
  FROM ordered
),

/* Calculate adjustment_factor only at roll dates */
adjustment_factors AS (
  SELECT
    *,
    CASE
      WHEN contract_symbol != next_contract_symbol THEN next_price - price
      ELSE 0
    END AS adjustment_factor
  FROM roll_adjustments
),

/* Applies the cumulative adjustments to create a back-adjusted continuous price series. */
cumulative_adjustments AS (
  SELECT
    *,
    SUM(adjustment_factor) OVER (
      ORDER BY contract_year, month_order, row_num DESC
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS cumulative_adjustment
  FROM adjustment_factors
),

final_series AS (
  SELECT
    trading_datetime,
    contract_symbol,
    contract_year,
    month_order,
    row_num,
    price,
    price + cumulative_adjustment AS adjusted_price,
    cumulative_adjustment,
    volume
  FROM cumulative_adjustments
)

/* Returns the continuous price series with both the original and adjusted prices. */
SELECT
  contract_symbol,
  trading_datetime,
  contract_year,
  month_order,
  row_num,
  price          AS unadjusted_price,
  adjusted_price AS price,
  cumulative_adjustment,
  volume
FROM final_series
ORDER BY contract_year, month_order, row_num
