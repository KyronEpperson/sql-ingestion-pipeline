from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3262599
table_name = "Stablecoin_Circulating_Supply"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)


'''
with 
base as (
select day, symbol, supply
from dune."21co".result_solana_tokenization
where symbol in ('USDC', 'USDT','USDP','PYUSD','USDY','USDe')
and day>=date(now() - interval '365' day)
union all
select  
day,
symbol,
supply
from query_3561684
where day>=date(now() - interval '365' day)
)

select 
*, supply-lag(supply, 1) over (partition by symbol order by day) as delta from base
'''