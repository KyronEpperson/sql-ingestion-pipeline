from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5918788
table_name = "PERPS_Overview2"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH
-- 🔹 Open Interest (OI) data
ois AS (
    WITH json_data AS (
        SELECT json_parse(http_get('https://api.llama.fi/overview/open-interest')) AS parsed_data
    ),
    total_data_chart_breakdown AS (
        SELECT json_extract(parsed_data, '$.totalDataChartBreakdown') AS breakdown_array
        FROM json_data
    ),
    unnested_data AS (
        SELECT
            from_unixtime(CAST(json_extract_scalar(item, '$[0]') AS bigint)) AS day,
            json_extract(item, '$[1]') AS product_oi_json
        FROM total_data_chart_breakdown
        CROSS JOIN UNNEST(CAST(json_extract(breakdown_array, '$') AS array(json))) AS t(item)
    ),
    expanded_data AS (
        SELECT
            day,
            CASE
                WHEN key IN ('dYdX V3', 'dYdX V4') THEN 'dYdX (V3+V4)'
                ELSE key
            END AS product_name,
            CAST(value AS double) AS oi
        FROM unnested_data
        CROSS JOIN UNNEST(CAST(product_oi_json AS map(varchar, double))) AS t(key, value)
    ),
    aggregated_data AS (
        SELECT
            day,
            product_name,
            SUM(oi) AS daily_oi
        FROM expanded_data
        GROUP BY day, product_name
    ),
    rolling_averages AS (
        SELECT
            a.day,
            a.product_name,
            AVG(a.daily_oi) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7d,
            AVG(a.daily_oi) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_30d,
            AVG(a.daily_oi) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS avg_90d,
            AVG(a.daily_oi) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS prev_avg_7d,
            AVG(a.daily_oi) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING) AS prev_avg_30d,
            AVG(a.daily_oi) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 179 PRECEDING AND 90 PRECEDING) AS prev_avg_90d
        FROM aggregated_data a
    ),
    final_data AS (
        SELECT DISTINCT
            product_name,
            FIRST_VALUE(avg_7d) OVER (PARTITION BY product_name ORDER BY day DESC) AS avg_7d_oi,
            FIRST_VALUE(avg_30d) OVER (PARTITION BY product_name ORDER BY day DESC) AS avg_30d_oi,
            FIRST_VALUE(avg_90d) OVER (PARTITION BY product_name ORDER BY day DESC) AS avg_90d_oi,
            FIRST_VALUE(CASE 
                WHEN prev_avg_7d IS NULL OR prev_avg_7d = 0 THEN NULL
                ELSE (avg_7d - prev_avg_7d) / prev_avg_7d 
            END) OVER (PARTITION BY product_name ORDER BY day DESC) AS delta_7d_pct_oi,
            FIRST_VALUE(CASE 
                WHEN prev_avg_30d IS NULL OR prev_avg_30d = 0 THEN NULL
                ELSE (avg_30d - prev_avg_30d) / prev_avg_30d 
            END) OVER (PARTITION BY product_name ORDER BY day DESC) AS delta_30d_pct_oi,
            FIRST_VALUE(CASE 
                WHEN prev_avg_90d IS NULL OR prev_avg_90d = 0 THEN NULL
                ELSE (avg_90d - prev_avg_90d) / prev_avg_90d 
            END) OVER (PARTITION BY product_name ORDER BY day DESC) AS delta_90d_pct_oi
        FROM rolling_averages
    ),
    total_ois AS (
        SELECT
            SUM(avg_7d_oi) AS total_avg_7d_oi,
            SUM(avg_30d_oi) AS total_avg_30d_oi,
            SUM(avg_90d_oi) AS total_avg_90d_oi
        FROM final_data
    ),
    final_data_with_market_share AS (
        SELECT
            f.*,
            f.avg_7d_oi / NULLIF(t.total_avg_7d_oi, 0) AS market_share_7d_oi,
            f.avg_30d_oi / NULLIF(t.total_avg_30d_oi, 0) AS market_share_30d_oi,
            f.avg_90d_oi / NULLIF(t.total_avg_90d_oi, 0) AS market_share_90d_oi
        FROM final_data f
        CROSS JOIN total_ois t
    )
    SELECT * FROM final_data_with_market_share
),

-- 🔹 Volume data (unchanged)
volumes AS (
    WITH json_data AS (
        SELECT json_parse(http_get('https://api.llama.fi/overview/derivatives')) AS parsed_data
    ),
    total_data_chart_breakdown AS (
        SELECT json_extract(parsed_data, '$.totalDataChartBreakdown') AS breakdown_array
        FROM json_data
    ),
    unnested_data AS (
        SELECT
            from_unixtime(CAST(json_extract_scalar(item, '$[0]') AS bigint)) AS day,
            json_extract(item, '$[1]') AS product_volumes_json
        FROM total_data_chart_breakdown
        CROSS JOIN UNNEST(CAST(json_extract(breakdown_array, '$') AS array(json))) AS t(item)
    ),
    expanded_data AS (
        SELECT
            day,
            CASE
                WHEN key IN ('dYdX V3', 'dYdX V4') THEN 'dYdX (V3+V4)'
                ELSE key
            END AS product_name,
            CAST(value AS double) AS volume
        FROM unnested_data
        CROSS JOIN UNNEST(CAST(product_volumes_json AS map(varchar, double))) AS t(key, value)
    ),
    aggregated_data AS (
        SELECT 
            day,
            product_name,
            SUM(volume) AS daily_volume
        FROM expanded_data
        GROUP BY day, product_name
    ),
    rolling_averages AS (
        SELECT
            a.day,
            a.product_name,
            AVG(a.daily_volume) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7d,
            AVG(a.daily_volume) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_30d,
            AVG(a.daily_volume) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS avg_90d,
            AVG(a.daily_volume) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS prev_avg_7d,
            AVG(a.daily_volume) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING) AS prev_avg_30d,
            AVG(a.daily_volume) OVER (PARTITION BY a.product_name ORDER BY a.day ROWS BETWEEN 179 PRECEDING AND 90 PRECEDING) AS prev_avg_90d
        FROM aggregated_data a
    ),
    final_data AS (
        SELECT DISTINCT
            product_name,
            FIRST_VALUE(avg_7d) OVER (PARTITION BY product_name ORDER BY day DESC) AS avg_7d,
            FIRST_VALUE(avg_30d) OVER (PARTITION BY product_name ORDER BY day DESC) AS avg_30d,
            FIRST_VALUE(avg_90d) OVER (PARTITION BY product_name ORDER BY day DESC) AS avg_90d,
            FIRST_VALUE(CASE 
                WHEN prev_avg_7d IS NULL OR prev_avg_7d = 0 THEN NULL
                ELSE (avg_7d - prev_avg_7d) / prev_avg_7d 
            END) OVER (PARTITION BY product_name ORDER BY day DESC) AS delta_7d_pct,
            FIRST_VALUE(CASE 
                WHEN prev_avg_30d IS NULL OR prev_avg_30d = 0 THEN NULL
                ELSE (avg_30d - prev_avg_30d) / prev_avg_30d 
            END) OVER (PARTITION BY product_name ORDER BY day DESC) AS delta_30d_pct,
            FIRST_VALUE(CASE 
                WHEN prev_avg_90d IS NULL OR prev_avg_90d = 0 THEN NULL
                ELSE (avg_90d - prev_avg_90d) / prev_avg_90d 
            END) OVER (PARTITION BY product_name ORDER BY day DESC) AS delta_90d_pct
        FROM rolling_averages
    ),
    total_volumes AS (
        SELECT
            SUM(avg_7d) AS total_avg_7d,
            SUM(avg_30d) AS total_avg_30d,
            SUM(avg_90d) AS total_avg_90d
        FROM final_data
    ),
    final_data_with_market_share AS (
        SELECT
            f.*,
            f.avg_7d / NULLIF(t.total_avg_7d, 0) AS market_share_7d,
            f.avg_30d / NULLIF(t.total_avg_30d, 0) AS market_share_30d,
            f.avg_90d / NULLIF(t.total_avg_90d, 0) AS market_share_90d
        FROM final_data f
        CROSS JOIN total_volumes t
    )
    SELECT * FROM final_data_with_market_share
)

-- 🔹 Combine OI + Volume
SELECT 
    v.product_name,
    v.avg_7d,
    v.avg_30d,
    v.avg_90d,
    v.delta_7d_pct,
    v.delta_30d_pct,
    v.delta_90d_pct,
    v.market_share_7d,
    v.market_share_30d,
    v.market_share_90d,
    o.avg_7d_oi AS average_oi,
    CASE 
        WHEN o.avg_7d_oi IS NULL THEN 'unknown'
        WHEN COALESCE(v.avg_7d, 0) = 0 THEN NULL
        ELSE CAST(COALESCE(v.avg_7d, 0) / NULLIF(o.avg_7d_oi, 0) AS VARCHAR)
    END AS volume_to_oi_ratio,
    CASE 
        WHEN o.avg_7d_oi IS NULL THEN 'unknown'
        WHEN COALESCE(v.avg_7d, 0) / NULLIF(o.avg_7d_oi, 0) > 6 THEN 'YES'
        WHEN COALESCE(v.avg_7d, 0) / NULLIF(o.avg_7d_oi, 0) BETWEEN 4 AND 6 THEN 'LIKELY'
        WHEN COALESCE(v.avg_7d, 0) / NULLIF(o.avg_7d_oi, 0) BETWEEN 2 AND 4 THEN 'unlikely'
        WHEN COALESCE(v.avg_7d, 0) / NULLIF(o.avg_7d_oi, 0) < 2 THEN 'no'
        ELSE 'unlikely'
    END AS label
FROM volumes v
LEFT JOIN ois o ON v.product_name = o.product_name
ORDER BY v.avg_7d DESC;

'''