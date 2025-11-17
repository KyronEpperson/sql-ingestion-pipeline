from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3506714
table_name = "Solana_NFT_Volume"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
select
date_trunc(
(case 
when '{{NFT Timeframe}}' = '1 Year' then 'day'
when '{{NFT Timeframe}}' = '2 Years' then 'week'
when '{{NFT Timeframe}}' = 'All-Time' then 'month'
end), block_time) as time, 
project, sum(amount_usd) as volume
from nft_solana.trades
where date(block_date)>=
(case 
when '{{NFT Timeframe}}' = '1 Year' then date(now() - interval '1' year)
when '{{NFT Timeframe}}' = '2 Years' then date(now() - interval '2' year)
when '{{NFT Timeframe}}' = 'All-Time' then date(now() - interval '20' year)
end)
group by 1,2
'''