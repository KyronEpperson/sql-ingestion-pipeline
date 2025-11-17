from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3759856
table_name = "PUMPFUN_Daily_Revenue"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
WITH excluded_transactions AS (
  SELECT DISTINCT tx_id
  FROM solana.account_activity
  WHERE tx_success = TRUE
    AND address IN (
      '49AdQfhKyVgWKb1HPi6maQxm5tqJasePR9K6Mn67hEYA',
      'EkuimaBYybHvviYjtMXcnC7eg6WQmzLriDPtvh98fjRg',
      'CL9jPThhYnxvPSWNLhR4J7in13WvtMXXBGCe8LEhipmj',
      '94qWNrtmfn42h3ZjUZwWvK1MEo9uVmmrBPd2hpNjYDjb',
      '7xQYoUjUJF1Kg6WVczoTAkaNhn5syQYcbvjmFrhjWpx',
      'BWXT6RUhit9FfJQM3pBmqeFLPYmuxgmyhMGC5sGr8RbA',
      'Bvtgim23rfocUzxVX9j9QFxTbBnH8JZxnaGLCEkXvjKS',
      'FGptqdxjahafaCzpZ1T6EDtCzYMv7Dyn5MgBLyB3VUFW',
      'X5QPJcpph4mBAJDzc4hRziFftSbcygV59kRb2Fu6Je1',
      '7GFUN3bWzJMKMRZ34JLsvcqdssDbXnp589SiE33KVwcC'
    )
    AND balance_change < 0
),

daily_revenue AS (
  SELECT 
    sa.block_date AS dt, 
    SUM(sa.balance_change) / 1e9 AS daily_revenue_sol 
  FROM solana.account_activity sa
  LEFT JOIN excluded_transactions et ON sa.tx_id = et.tx_id
  WHERE sa.tx_success = TRUE
    AND sa.block_date < CURRENT_DATE
    AND sa.address IN (
      'CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM', 
      '62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV',
      'FWsW1xNtWscwNmKv6wVsU1iTzRN6wmmk3MjxRP5tT7hz',
      '7hTckgnGnLQR6sdH7YkqFTAA7VwTfYFaZ6EhEsU3saCX',
      'AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY',
      '9rPYyANsfQZw3DnDmKE3YCQF5E8oD89UXoHn9JFEhJUz',
      'G5UZAVbAf46s7cKWoyKu8kYTip9DGTpbLZ2qa9Aq69dP',
      '7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ'
    )
    AND sa.balance_change > 0
    AND et.tx_id IS NULL 
  GROUP BY sa.block_date
),

exchange_rates AS (
  SELECT 
    day AS dt, 
    price AS avg_sol_to_usd 
  FROM prices.usd_daily
  WHERE symbol = 'SOL'
    AND blockchain = 'solana'
    AND day < CURRENT_DATE
)

SELECT 
  r.dt AS date,
  r.daily_revenue_sol,
  r.daily_revenue_sol * e.avg_sol_to_usd AS daily_revenue_usd,
  SUM(r.daily_revenue_sol * e.avg_sol_to_usd) OVER (ORDER BY r.dt ASC) AS cumulative_revenue_usd,
  SUM(r.daily_revenue_sol * e.avg_sol_to_usd) OVER () AS total_revenue_usd
FROM daily_revenue r
JOIN exchange_rates e ON r.dt = e.dt
ORDER BY r.dt DESC;
'''