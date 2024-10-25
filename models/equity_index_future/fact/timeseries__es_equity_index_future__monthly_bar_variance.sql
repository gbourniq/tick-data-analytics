
-- Transforms weekly bar counts into a monthly view
-- Aggregates tick, volume, and dollar bar counts for each week
WITH weekly_counts AS (
  SELECT
    DATE_TRUNC('month', week_start) AS month_start,
    week_start,
    tick_bar_count,
    volume_bar_count,
    dollar_bar_count
  FROM {{ ref('timeseries__es_equity_index_future__weekly_bar_counts') }}
),

-- Calculates monthly statistics for each bar type (tick, volume, dollar)
-- Computes variance, average weekly count, and total count for each month
monthly_stats AS (
  SELECT
    month_start,
    'tick'                  AS bar_type,
    VAR_POP(tick_bar_count) AS bar_count_variance,
    AVG(tick_bar_count)     AS avg_monthly_bar_count,
    SUM(tick_bar_count)     AS total_bar_count
  FROM weekly_counts
  GROUP BY month_start

  UNION ALL

  SELECT
    month_start,
    'volume'                  AS bar_type,
    VAR_POP(volume_bar_count) AS bar_count_variance,
    AVG(volume_bar_count)     AS avg_monthly_bar_count,
    SUM(volume_bar_count)     AS total_bar_count
  FROM weekly_counts
  GROUP BY month_start

  UNION ALL

  SELECT
    month_start,
    'dollar'                  AS bar_type,
    VAR_POP(dollar_bar_count) AS bar_count_variance,
    AVG(dollar_bar_count)     AS avg_monthly_bar_count,
    SUM(dollar_bar_count)     AS total_bar_count
  FROM weekly_counts
  GROUP BY month_start
)

-- Retrieves and orders the monthly statistics
SELECT
  month_start,
  bar_type,
  bar_count_variance,
  avg_monthly_bar_count,
  total_bar_count
FROM monthly_stats
ORDER BY month_start, bar_type
