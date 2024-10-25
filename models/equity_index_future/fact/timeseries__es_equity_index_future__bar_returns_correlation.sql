-- This model computes the serial correlation of price returns for tick, volume, and dollar bars
-- Note: Serial correlation measures the relationship between an asset's return and its lagged return.
-- A positive serial correlation indicates trend continuation, while a negative one suggests mean reversion.
-- Values close to zero imply little to no serial correlation.

-- Combine data from all three bar types into a single CTE
WITH bar_data AS (
  SELECT
    bar_start_time,
    close,
    'tick' AS bar_type
  FROM {{ ref('int__es_equity_index_future__tick_bars') }}
  UNION ALL
  SELECT
    bar_start_time,
    close,
    'volume' AS bar_type
  FROM {{ ref('int__es_equity_index_future__volume_bars') }}
  UNION ALL
  SELECT
    bar_start_time,
    close,
    'dollar' AS bar_type
  FROM {{ ref('int__es_equity_index_future__dollars_traded_bars') }}
),

-- Calculate percentage return using the current and previous close prices
price_returns AS (
  SELECT
    bar_type,
    bar_start_time,
    (close - LAG(close) OVER (PARTITION BY bar_type ORDER BY bar_start_time)) / LAG(close) OVER (PARTITION BY bar_type ORDER BY bar_start_time) AS return_value
  FROM bar_data
),

-- Calculate lagged returns for each bar type
lagged_returns AS (
  SELECT
    bar_type,
    bar_start_time,
    return_value,
    LAG(return_value) OVER (PARTITION BY bar_type ORDER BY bar_start_time) AS lagged_return_value
  FROM price_returns
),

-- Calculate correlation between current return and previous return (serial correlation) for each bar type
SELECT
  bar_type,
  CORR(return_value, lagged_return_value) AS serial_correlation
FROM lagged_returns
GROUP BY bar_type
