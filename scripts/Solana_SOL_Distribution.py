from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3255941
table_name = "Solana_SOL_Distribution"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
with base as (
select 
distinct
address, 
case 
when sol_balance>0 and sol_balance<= 1 then 'A) 0-1 SOL'
when sol_balance>1 and sol_balance<= 100 then 'B) 1-100 SOL'
when sol_balance>100 and  sol_balance<= 10000 then 'C) 100-10k SOL'
when sol_balance>10000 and  sol_balance<= 100000 then 'D) 10k-100k SOL'
when sol_balance>100000 and  sol_balance<= 1000000 then 'E) 100k-1M SOL'
when sol_balance>1000000 and  sol_balance<= 10000000 then 'F) 1M-10M SOL'
when sol_balance>10000000 then 'G) >10M SOL' 
end as groups,
sol_balance
from solana_utils.latest_balances)

select
groups,
sum(sol_balance) as sol_balance,
count(distinct address) as addr_count
from base
where groups is not null
group by 1
order by 1 asc
'''