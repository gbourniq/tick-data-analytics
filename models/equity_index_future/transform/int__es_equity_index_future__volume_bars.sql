-- Calculate the total number of bars and total volume from tick bars
WITH tick_bar_stats AS (
  SELECT
    COUNT(*)    AS num_bars,
    SUM(volume) AS total_volume
  FROM {{ ref('int__es_equity_index_future__tick_bars') }}
),

-- Calculate the average volume per bar to determine the target volume for each volume bar
avg_volume_per_bar AS (
  SELECT ROUND(total_volume::numeric / num_bars, 0) AS avg_volume_per_bar
  FROM tick_bar_stats
),

-- Select and rename relevant columns from continuous futures data
numbered_ticks AS (
  SELECT
    contract_symbol,
    trading_datetime,
    row_num,
    price,
    volume AS tick_volume
  FROM {{ ref('int__es_equity_index_future__continuous') }}
),

-- Calculate cumulative volume and assign volume groups
volume_groups AS (
  SELECT
    *,
    SUM(tick_volume) OVER (PARTITION BY contract_symbol ORDER BY row_num)                                                              AS cumulative_volume,
    FLOOR(SUM(tick_volume) OVER (PARTITION BY contract_symbol ORDER BY row_num) / (SELECT avg_volume_per_bar FROM avg_volume_per_bar)) AS volume_group
  FROM numbered_ticks
),

-- Perform window calculations to compute bar statistics (OHLC, volume, tick count)
window_calcs AS (
  SELECT
    *,
    FIRST_VALUE(trading_datetime) OVER (PARTITION BY contract_symbol, volume_group ORDER BY row_num)                                                         AS bar_start_time,
    LAST_VALUE(trading_datetime) OVER (PARTITION BY contract_symbol, volume_group ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS bar_end_time,
    FIRST_VALUE(price) OVER (PARTITION BY contract_symbol, volume_group ORDER BY row_num)                                                                    AS open,
    MAX(price) OVER (PARTITION BY contract_symbol, volume_group)                                                                                             AS high,
    MIN(price) OVER (PARTITION BY contract_symbol, volume_group)                                                                                             AS low,
    LAST_VALUE(price) OVER (PARTITION BY contract_symbol, volume_group ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)            AS close,
    SUM(tick_volume) OVER (PARTITION BY contract_symbol, volume_group)                                                                                       AS volume,
    COUNT(*) OVER (PARTITION BY contract_symbol, volume_group)                                                                                               AS tick_count,
    FIRST_VALUE(row_num) OVER (PARTITION BY contract_symbol, volume_group ORDER BY row_num)                                                                  AS start_row_num,
    LAST_VALUE(row_num) OVER (PARTITION BY contract_symbol, volume_group ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)          AS end_row_num
  FROM volume_groups
),

-- Select distinct rows to create the final volume bars
volume_bars AS (
  SELECT DISTINCT
    contract_symbol,
    volume_group,
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
)

-- Output the volume bars, ordered by contract symbol and bar start time
SELECT
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
FROM volume_bars
ORDER BY contract_symbol, bar_start_time
