from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3240305
table_name = "Top_1000_SOL_Holders"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
with top_address as (select distinct address, sol_balance
from solana_utils.latest_balances
order by 2 desc
limit 1000),

stake_account as (
SELECT account_stakeAccount as stake FROM stake_program_solana.stake_call_Initialize
UNION 
SELECT account_stakeAccount as stake FROM stake_program_solana.stake_call_InitializeChecked
union 
select distinct account_newAccount as stake from system_program_solana.system_program_call_CreateAccount
UNION 
select distinct account_newAccount as stake from system_program_solana.system_program_call_CreateAccountWithSeed
union 
select distinct account_stakeAccount  as stake from stake_program_solana.stake_call_DelegateStake
union
SELECT destination as stake FROm staking_solana.stake_actions
WHERE action = 'merge' or action = 'split'
),

supply as (
select total_supply
from query_3199137
where day=date(now())
)

select rank() over (order by sol_balance desc) as ranking,
CONCAT('<a href="https://solscan.io/account/', cast(a.address as varchar), '" target="_blank">',cast(a.address as varchar), '</a>') as address,
entity as label, 
case 
when a.address in (select stake from stake_account) then 'Stake Account'
else 'Account'
end as wallet_type,
sol_balance,  
sol_balance*price as sol_usd_balance,
sol_balance/total_supply as percentage
from supply, top_address a
left join query_3240310 b
on a.address=b.address
left join prices.usd_latest
on contract_address is null
and symbol = 'SOL'
order by sol_balance desc
'''