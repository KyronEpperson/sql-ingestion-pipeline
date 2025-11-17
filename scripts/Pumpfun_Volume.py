from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4160369
table_name = "Pumpfun_Volume"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
WITH pumpdotfun_volume AS (
    SELECT 
        DATE_TRUNC('week', block_time) AS dt, 
        SUM(amount_usd) AS weekly_volume_usd,
        COUNT(*) AS total_trades,
        'Pumpdotfun' AS platform
    FROM dex_solana.trades
    WHERE 
        project = 'pumpdotfun'
        AND block_time >= DATE_ADD('week', -100, NOW())
        AND DATE_TRUNC('week', block_time) < DATE_TRUNC('week', NOW()) 
    GROUP BY DATE_TRUNC('week', block_time)
)

SELECT 
    dt AS date,
    weekly_volume_usd AS weekly_volume,
    weekly_volume_usd AS weekly_volume_usd,
    SUM(weekly_volume_usd) OVER (PARTITION BY platform ORDER BY dt ASC) AS cumulative_volume_usd,
    platform
FROM pumpdotfun_volume

ORDER BY date DESC, platform ASC;
'''