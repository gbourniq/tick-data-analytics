{% set ticks_per_bar = 100000 %}  -- Adjust this value to change the bar size

-- Select and rename relevant columns from continuous futures data
WITH numbered_ticks AS (
  SELECT
    contract_symbol,
    trading_datetime,
    contract_year,
    month_order,
    row_num,
    unadjusted_price,
    price,
    cumulative_adjustment,
    volume AS tick_volume
  FROM {{ ref('int__es_equity_index_future__continuous') }}
),

-- Group ticks into bars based on the specified number of ticks per bar
with_tick_groups AS (
  SELECT
    *,
    FLOOR((ROW_NUMBER() OVER (PARTITION BY contract_symbol ORDER BY row_num) - 1) / {{ ticks_per_bar }}) AS tick_group
  FROM numbered_ticks
),

-- Perform window calculations to compute bar statistics (OHLC, volume, tick count)
window_calcs AS (
  SELECT
    *,
    FIRST_VALUE(trading_datetime) OVER (PARTITION BY contract_symbol, tick_group ORDER BY row_num)                                                         AS bar_start_time,
    LAST_VALUE(trading_datetime) OVER (PARTITION BY contract_symbol, tick_group ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS bar_end_time,
    FIRST_VALUE(price) OVER (PARTITION BY contract_symbol, tick_group ORDER BY row_num)                                                                    AS open,
    MAX(price) OVER (PARTITION BY contract_symbol, tick_group)                                                                                             AS high,
    MIN(price) OVER (PARTITION BY contract_symbol, tick_group)                                                                                             AS low,
    LAST_VALUE(price) OVER (PARTITION BY contract_symbol, tick_group ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)            AS close,
    SUM(tick_volume) OVER (PARTITION BY contract_symbol, tick_group)                                                                                       AS volume,
    COUNT(*) OVER (PARTITION BY contract_symbol, tick_group)                                                                                               AS tick_count,
    FIRST_VALUE(row_num) OVER (PARTITION BY contract_symbol, tick_group ORDER BY row_num)                                                                  AS start_row_num,
    LAST_VALUE(row_num) OVER (PARTITION BY contract_symbol, tick_group ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)          AS end_row_num
  FROM with_tick_groups
),

-- Select distinct rows to create the final tick bars
SELECT DISTINCT
  contract_symbol,
  bar_start_time,
  bar_end_time,
  open,
  high,
  low,
  close,
  volume,
  tick_count,
  start_row_num,
  end_row_num
FROM window_calcs
ORDER BY contract_symbol, bar_start_time
