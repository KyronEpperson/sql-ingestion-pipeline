from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3500326
table_name = "Solana_DEX_Volume"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
-- select date_trunc('day',block_date) as week, blockchain, sum(amount_usd) as volume
-- from dex.trades
-- where block_date>=date(now() - interval '1' year)
-- group by 1,2
-- union all
select
date_trunc(
(case 
when '{{DEX Timeframe}}' = '1 Year' then 'day'
when '{{DEX Timeframe}}' = '2 Years' then 'week'
when '{{DEX Timeframe}}' = 'All-Time' then 'month'
end), block_time) as time, 
project, sum(amount_usd) as volume
from dex_solana.trades
where date(block_time)>= 
(case 
when '{{DEX Timeframe}}' = '1 Year' then date(now() - interval '1' year)
when '{{DEX Timeframe}}' = '2 Years' then date(now() - interval '2' year)
when '{{DEX Timeframe}}' = 'All-Time' then date(now() - interval '20' year)
end)
AND token_bought_mint_address != '4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi' and token_sold_mint_address != '4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi'
group by 1,2
'''