from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3225274
table_name = "24h_solana_programs"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
with latest as (select 
namespace,
executing_account, 
count(*) as calls,
count(distinct tx_signer) as users
from solana.instruction_calls a
left join solana.programs b
on a.executing_account=b.program_id
where block_time>=now() - interval '24' hour
and tx_success = true
group by 1,2
having count(*) > 10000
order by 3 desc
limit 100),

delta_1d as (
select 
executing_account, 
count(*) as calls_delta_1d,
count(distinct tx_signer) as users_delta_1d
from solana.instruction_calls
where block_date=date(now() - interval '1' day)
and tx_success = true
and executing_account in (select executing_account from latest)
group by 1
),

delta_7d as (
select
executing_account, 
count(*) as calls_delta_7d,
count(distinct tx_signer) as users_delta_7d
from solana.instruction_calls
where block_date=date(now() - interval '7' day)
and tx_success = true
and executing_account in (select executing_account from latest)
group by 1
)

-- delta_30d as (
-- executing_account, 
-- count(*) as calls_delta_30d,
-- count(distinct tx_signer) as users_delta_30d
-- from solana.instruction_calls
-- where block_date=date(now() - interval '30' day)
-- and tx_success = true
-- and executing_account in (select executing_account from latest)
-- group by 1
-- )

select
rank() over (order by calls desc) as ranking,
case
when namespace is null and a.executing_account = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL' then 'Associated Token Account Program'
when namespace is null and a.executing_account = 'TLPv2tuSVvn3fSk8RgW3yPddkp5oFivzZV3rA9hQxtX' then 'Tulip Protocol V2 Vaults'
when namespace is null and a.executing_account = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA' then 'Marginfi V2'
when namespace is null and a.executing_account = 'cjg3oHmg9uuPsP8D6g29NWvhySJkdYdAo9D25PRbKXJ' then 'Chainlink Data Store Program '
else namespace end as program_name,
CONCAT('<a href="https://solana.fm/address/', a.executing_account, '" target="_blank">', a.executing_account, '</a>') as executing_account,
cast(calls as double) as calls, 
cast(users as double) as users, 
cast(calls as double)/cast(calls_delta_1d as double)-1 as calls_delta_1d,
cast(users as double)/cast(users_delta_1d as double)-1 as users_delta_1d,
cast(calls as double)/cast(calls_delta_7d as double)-1 as calls_delta_7d,
cast(users as double)/cast(users_delta_7d as double)-1 as users_delta_7d
from latest a 
left join delta_1d b
on a.executing_account=b.executing_account
left join delta_7d c
on a.executing_account=c.executing_account
'''