from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5931558
table_name = "PERPS_oi_inter"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
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
        json_extract(item, '$[1]') AS product_volumes_json
    FROM total_data_chart_breakdown
    CROSS JOIN UNNEST(CAST(json_extract(breakdown_array, '$') AS array(json))) AS t(item)
),
expanded_data AS (
    SELECT
        date_trunc('week', day) AS week_start_date,
        CASE
            WHEN key IN ('dYdX V3', 'dYdX V4') THEN 'dYdX (V3+V4)'
            ELSE key
        END AS product_name,
        CAST(value AS double) AS volume
    FROM unnested_data
    CROSS JOIN UNNEST(CAST(product_volumes_json AS map(varchar, double))) AS t(key, value)
    WHERE key NOT IN ('SynFutures V3', 'edgeX', 'Satori Perp', 'APX Finance', 'Contango V2', 'Kalshi',
    'HoldStation DeFutures', 'ADEN', 'HMX', 'Aark Digital', 'JOJO', 'KiloEx', 'MYX Finance')  -- Exclude specified platforms
),
aggregated_data AS (
    SELECT 
        week_start_date,
        product_name,
        SUM(volume) AS volume
    FROM expanded_data
    GROUP BY week_start_date, product_name
),
ranked_week AS (
    SELECT
        product_name,
        ROW_NUMBER() OVER (ORDER BY volume DESC) AS rank
    FROM aggregated_data
    WHERE week_start_date = date_trunc('week', CURRENT_DATE)
),
final_data AS (
    SELECT
        a.week_start_date,
        CASE 
            WHEN r.rank IS NOT NULL AND r.rank <= 10 THEN a.product_name
            ELSE 'Other'
        END AS product_name,
        SUM(a.volume) AS volume
    FROM aggregated_data a
    LEFT JOIN ranked_week r
      ON a.product_name = r.product_name
    GROUP BY a.week_start_date, 
             CASE 
                 WHEN r.rank IS NOT NULL AND r.rank <= 10 THEN a.product_name
                 ELSE 'Other'
             END
)
SELECT 
    week_start_date AS week,
    product_name,
    volume
FROM final_data
ORDER BY week, product_name;
'''