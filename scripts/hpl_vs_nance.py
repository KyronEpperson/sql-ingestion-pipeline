from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4276329
table_name = "hpl_vs_nance"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
WITH json_data AS (
    SELECT json_parse(http_get('https://api.llama.fi/summary/derivatives/hyperliquid')) AS parsed_data
),
total_data_chart_breakdown AS (
    SELECT json_extract(parsed_data, '$.totalDataChartBreakdown') AS breakdown_array
    FROM json_data
),
hyperliquid_data AS (
    SELECT
        from_unixtime(CAST(json_extract_scalar(item, '$[0]') AS bigint)) AS day,
        CASE 
            WHEN DATE(from_unixtime(CAST(json_extract_scalar(item, '$[0]') AS bigint))) = CURRENT_DATE
            THEN CAST(json_extract_scalar(json_extract(item, '$[1]'), '$["Hyperliquid L1"]["Hyperliquid Perps"]') AS double) * (EXTRACT(HOUR FROM CURRENT_TIMESTAMP) / 24.0)
            ELSE CAST(json_extract_scalar(json_extract(item, '$[1]'), '$["Hyperliquid L1"]["Hyperliquid Perps"]') AS double)
        END AS hyperliquid_volume
    FROM total_data_chart_breakdown
    CROSS JOIN UNNEST(CAST(json_extract(breakdown_array, '$') AS array(json))) AS t(item)
    WHERE EXTRACT(YEAR FROM from_unixtime(CAST(json_extract_scalar(item, '$[0]') AS bigint))) >= 2023
),
binance_tickers AS (
    SELECT * FROM (VALUES
        ('ETHUSDT'), ('DOGEUSDT'), ('SOLUSDT'), ('1000PEPEUSDT'), ('BTCUSDC'),
        ('1000SHIBUSDT'), ('XRPUSDT'), ('NEIROUSDT'), ('SUIUSDT'), ('BNBUSDT'), 
        ('WIFUSDT'), ('ACTUSDT'), ('WLDUSDT'), ('ADAUSDT'), ('APTUSDT'), 
        ('ETHUSDC'), ('1000BONKUSDT'), ('AVAXUSDT'), ('DOGEUSDC'), ('TRUMPUSDT'),
        ('MOODENGUSDT'), ('ORDIUSDT'), ('1000FLOKIUSDT'), ('NEARUSDT'), ('GOATUSDT'),
        ('BTCUSDT'), ('PNUTUSDT'), ('UNIUSDT'), ('POPCATUSDT'), ('BCHUSDT')
    ) AS t(symbol)
),
binance_api_responses AS (
    SELECT
        symbol,
        json_parse(http_get(concat('https://fapi.binance.com/fapi/v1/klines?symbol=', symbol, '&interval=1d&limit=1000'))) AS klines_data
    FROM binance_tickers
),
binance_data AS (
    SELECT 
        from_unixtime(CAST(json_extract_scalar(kline, '$[0]') AS bigint) / 1000) AS day,
        SUM(CAST(json_extract_scalar(kline, '$[5]') AS double) * CAST(json_extract_scalar(kline, '$[1]') AS double)) AS binance_volume
    FROM binance_api_responses
    CROSS JOIN UNNEST(CAST(klines_data AS array(json))) AS t(kline)
    WHERE EXTRACT(YEAR FROM from_unixtime(CAST(json_extract_scalar(kline, '$[0]') AS bigint) / 1000)) >= 2023
    GROUP BY from_unixtime(CAST(json_extract_scalar(kline, '$[0]') AS bigint) / 1000)
),
combined_daily_data AS (
    SELECT
        COALESCE(h.day, b.day) AS day,
        COALESCE(h.hyperliquid_volume, 0) AS hyperliquid_volume,
        COALESCE(b.binance_volume, 0)*1.3 AS binance_volume
    FROM hyperliquid_data h
    FULL OUTER JOIN binance_data b ON DATE(h.day) = DATE(b.day)
),
weekly_aggregated_data AS (
    SELECT 
        DATE_TRUNC('week', day) AS week,
        SUM(hyperliquid_volume) AS weekly_hyperliquid_volume,
        SUM(binance_volume) AS weekly_binance_volume,
        SUM(hyperliquid_volume) / NULLIF(SUM(binance_volume), 0) AS market_share
    FROM combined_daily_data
    GROUP BY DATE_TRUNC('week', day)
)
SELECT
    week,
    weekly_hyperliquid_volume,
    weekly_binance_volume,
    ROUND(market_share, 4) AS "market share (Hyperliquid / Binance)"
FROM weekly_aggregated_data
WHERE weekly_hyperliquid_volume > 0 AND weekly_binance_volume > 0
ORDER BY week
'''