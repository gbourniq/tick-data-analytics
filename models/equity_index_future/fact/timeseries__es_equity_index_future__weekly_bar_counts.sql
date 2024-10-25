WITH tick_bars AS (
  SELECT
    DATE_TRUNC('week', bar_start_time) AS week_start,
    COUNT(*)                           AS tick_bar_count
  FROM {{ ref('int__es_equity_index_future__tick_bars') }}
  GROUP BY DATE_TRUNC('week', bar_start_time)
),

volume_bars AS (
  SELECT
    DATE_TRUNC('week', bar_start_time) AS week_start,
    COUNT(*)                           AS volume_bar_count
  FROM {{ ref('int__es_equity_index_future__volume_bars') }}
  GROUP BY DATE_TRUNC('week', bar_start_time)
),

dollar_bars AS (
  SELECT
    DATE_TRUNC('week', bar_start_time) AS week_start,
    COUNT(*)                           AS dollar_bar_count
  FROM {{ ref('int__es_equity_index_future__dollars_traded_bars') }}
  GROUP BY DATE_TRUNC('week', bar_start_time)
)

SELECT
  COALESCE(t.week_start, v.week_start, d.week_start) AS week_start,
  COALESCE(t.tick_bar_count, 0)                      AS tick_bar_count,
  COALESCE(v.volume_bar_count, 0)                    AS volume_bar_count,
  COALESCE(d.dollar_bar_count, 0)                    AS dollar_bar_count
FROM tick_bars t
FULL OUTER JOIN volume_bars v ON t.week_start = v.week_start
FULL OUTER JOIN dollar_bars d ON t.week_start = d.week_start
ORDER BY week_start
