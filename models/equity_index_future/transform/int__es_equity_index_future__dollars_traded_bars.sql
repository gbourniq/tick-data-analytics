-- Calculate the total number of bars and total dollar value from continuous futures data
WITH tick_bar_stats AS (
  SELECT
    COUNT(*)                                        AS num_bars,
    SUM(((open + high + low + close) / 4) * volume) AS total_dollar_value
  FROM {{ ref('int__es_equity_index_future__tick_bars') }}
),

-- Calculate the average dollar value per bar to determine the target dollar value for each dollar bar
avg_dollar_per_bar AS (
  SELECT ROUND(total_dollar_value::numeric / num_bars, 0) AS avg_dollar_per_bar
  FROM tick_bar_stats
),

-- Select and rename relevant columns from continuous futures data
numbered_ticks AS (
  SELECT
    contract_symbol,
    trading_datetime,
    row_num,
    price,
    volume         AS tick_volume,
    price * volume AS dollars_traded
  FROM {{ ref('int__es_equity_index_future__continuous') }}
),

-- Calculate cumulative dollars traded and assign dollar groups
dollar_groups AS (
  SELECT
    *,
    SUM(dollars_traded) OVER (PARTITION BY contract_symbol ORDER BY row_num)                                                              AS cumulative_dollars,
    FLOOR(SUM(dollars_traded) OVER (PARTITION BY contract_symbol ORDER BY row_num) / (SELECT avg_dollar_per_bar FROM avg_dollar_per_bar)) AS dollar_group
  FROM numbered_ticks
),

-- Perform window calculations to compute bar statistics (OHLC, volume, tick count, dollars traded)
window_calcs AS (
  SELECT
    *,
    FIRST_VALUE(trading_datetime) OVER (PARTITION BY contract_symbol, dollar_group ORDER BY row_num)                                                         AS bar_start_time,
    LAST_VALUE(trading_datetime) OVER (PARTITION BY contract_symbol, dollar_group ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS bar_end_time,
    FIRST_VALUE(price) OVER (PARTITION BY contract_symbol, dollar_group ORDER BY row_num)                                                                    AS open,
    MAX(price) OVER (PARTITION BY contract_symbol, dollar_group)                                                                                             AS high,
    MIN(price) OVER (PARTITION BY contract_symbol, dollar_group)                                                                                             AS low,
    LAST_VALUE(price) OVER (PARTITION BY contract_symbol, dollar_group ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)            AS close,
    SUM(tick_volume) OVER (PARTITION BY contract_symbol, dollar_group)                                                                                       AS volume,
    SUM(dollars_traded) OVER (PARTITION BY contract_symbol, dollar_group)                                                                                    AS total_dollars_traded,
    COUNT(*) OVER (PARTITION BY contract_symbol, dollar_group)                                                                                               AS tick_count,
    FIRST_VALUE(row_num) OVER (PARTITION BY contract_symbol, dollar_group ORDER BY row_num)                                                                  AS start_row_num,
    LAST_VALUE(row_num) OVER (PARTITION BY contract_symbol, dollar_group ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)          AS end_row_num
  FROM dollar_groups
),

-- Select distinct rows to create the final dollars-traded bars
dollars_traded_bars AS (
  SELECT DISTINCT
    contract_symbol,
    dollar_group,
    bar_start_time,
    bar_end_time,
    open,
    high,
    low,
    close,
    volume,
    total_dollars_traded,
    tick_count,
    start_row_num,
    end_row_num
  FROM window_calcs
)

-- Output the dollars-traded bars, ordered by contract symbol and bar start time
SELECT
  contract_symbol,
  bar_start_time,
  bar_end_time,
  open,
  high,
  low,
  close,
  volume,
  total_dollars_traded,
  tick_count,
  start_row_num,
  end_row_num
FROM dollars_traded_bars
ORDER BY contract_symbol, bar_start_time
