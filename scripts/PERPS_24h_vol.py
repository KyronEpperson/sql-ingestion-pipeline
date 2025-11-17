from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4095171
table_name = "PERPS_24h_vol"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH json_data AS (
  SELECT
    JSON_PARSE(HTTP_GET('https://api.llama.fi/overview/derivatives')) AS parsed_data
), total_data_chart_breakdown AS (
  SELECT
    JSON_EXTRACT(parsed_data, '$.totalDataChartBreakdown') AS breakdown_array
  FROM json_data
), unnested_data AS (
  SELECT
    FROM_UNIXTIME(TRY_CAST(JSON_EXTRACT_SCALAR(item, '$[0]') AS BIGINT)) AS timestamp,
    JSON_EXTRACT(item, '$[1]') AS product_volumes_json
  FROM total_data_chart_breakdown
  CROSS JOIN UNNEST(TRY_CAST(JSON_EXTRACT(breakdown_array, '$') AS ARRAY(JSON))) AS t(item)
), expanded_data AS (
  SELECT
    DATE_TRUNC('hour', timestamp) AS hour_start,
    CASE WHEN key IN ('dYdX V3', 'dYdX V4') THEN 'dYdX (V3+V4)' ELSE key END AS product_name,
    TRY_CAST(value AS DOUBLE) AS volume
  FROM unnested_data
  CROSS JOIN UNNEST(TRY_CAST(product_volumes_json AS MAP(VARCHAR, DOUBLE))) AS t(key, value)
), aggregated_data AS (
  SELECT
    hour_start,
    product_name,
    SUM(volume) AS volume
  FROM expanded_data /* corrected condition to use hour_start instead of timestamp */
  WHERE
    hour_start >= CURRENT_TIMESTAMP - INTERVAL '24' hour
  GROUP BY
    hour_start,
    product_name
), labeled_data AS (
  SELECT
    hour_start,
    CASE WHEN volume >= 100000000 THEN product_name ELSE 'Other' END AS product_name,
    SUM(volume) AS volume
  FROM aggregated_data
  GROUP BY
    hour_start,
    CASE WHEN volume >= 100000000 THEN product_name ELSE 'Other' END
), filtered_data AS (
  SELECT
    hour_start AS hour,
    product_name,
    volume
  FROM labeled_data
  WHERE
    product_name <> 'Other1'
)
SELECT
  hour,
  product_name,
  volume
FROM filtered_data
ORDER BY
  hour,
  product_name
'''