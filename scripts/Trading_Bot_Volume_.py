from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4422946
table_name = "Trading_Bot_Volume_&_Fees"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH combined AS (
  SELECT 'BullX' AS bot_name, day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_bull_x_2_months
  UNION ALL
  SELECT 'GMGN', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_gmgn_2_months
  UNION ALL
  SELECT 'Photon', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_photon_2_months
  UNION ALL
  SELECT 'Axiom', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_axiom_2_months
  UNION ALL
  SELECT 'Shuriken', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_shuriken_2_months
  UNION ALL
  SELECT 'Mevx', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_mevx_2_months
  UNION ALL
  SELECT 'Vector', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_vector_2_months
  UNION ALL
  SELECT 'Nova', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_nova_2_months
  UNION ALL
  SELECT 'Bloom', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_bloom_2_months
  UNION ALL
  SELECT 'BonkBot', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_bonk_bot_2_months
  UNION ALL
  SELECT 'Pepe Boost', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_pepe_boost_2_months
  UNION ALL
  SELECT 'Sol Trading Bot', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_sol_trading_bot_2_months
  UNION ALL
  SELECT 'Trojan', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_trojan_2_months
  UNION ALL
  SELECT 'TradeWiz', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_trade_wiz_2_months
  UNION ALL
  SELECT 'Terminal (formerly Padre)', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_padre_2_months
  UNION ALL
  SELECT 'Maestro', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_maestro_2_mon
  UNION ALL
  SELECT 'Sigma', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_sigma_2_months
  UNION ALL
  SELECT 'fomo', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_fomo_2_months
  UNION ALL
  SELECT 'DSX', day, total_volume_usd, total_fees_usd FROM dune.trench_wizards.result_dsx_2_months
)

SELECT *
FROM combined
WHERE day >= CURRENT_DATE - INTERVAL '2' month
AND day < CURRENT_DATE
ORDER BY day DESC, bot_name;

'''