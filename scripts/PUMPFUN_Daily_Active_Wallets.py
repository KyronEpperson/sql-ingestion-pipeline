from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4903519
table_name = "PUMPFUN_Daily_Active_Wallets"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH first_seen AS (
  SELECT
    trader_id,
    MIN(DATE(block_time)) AS first_date
  FROM dex_solana.trades
  WHERE project = 'pumpdotfun'
  GROUP BY trader_id
),
daily_users AS (
  SELECT
    DATE(block_time) AS day,
    trader_id
  FROM dex_solana.trades
  WHERE project = 'pumpdotfun'
  GROUP BY 1, 2
),
tagged_users AS (
  SELECT
    du.day,
    du.trader_id,
    CASE
      WHEN du.day = fs.first_date THEN 'new'
      ELSE 'recurring'
    END AS user_type
  FROM daily_users du
  JOIN first_seen fs ON du.trader_id = fs.trader_id
)
SELECT
  day,
  COUNT(CASE WHEN user_type = 'new' THEN 1 END) AS new_users,
  COUNT(CASE WHEN user_type = 'recurring' THEN 1 END) AS recurring_users
FROM tagged_users
GROUP BY 1
ORDER BY 1 DESC

'''