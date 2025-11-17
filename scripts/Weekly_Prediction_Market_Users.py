from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5747911
table_name = "Weekly_Prediction_Market_Users"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
with myriad as (
with trades as (
SELECT evt_block_time, evt_tx_hash, user, contract_address, action, marketId, value, 'abstract' as chain  FROM myriad_abstract.predictionmarketv3_evt_marketactiontx
WHERE action IN (0,1)
UNION ALL 
SELECT evt_block_time, evt_tx_hash, user, contract_address, action, marketId, value, chain FROM myriad_multichain.predictionmarketv3_4_evt_marketactiontx
WHERE action IN (0,1)
UNION ALL 
SELECT evt_block_time, evt_tx_hash, user, contract_address, action, marketId, value, 'abstract' as chain  FROM myriad_abstract.predictionmarketv3_3_points_evt_marketactiontx
WHERE action IN (0,1)
UNION ALL 
SELECT evt_block_time, evt_tx_hash, user, contract_address, action, marketId, value, 'abstract' as chain  FROM myriad_abstract.predictionmarketv4_evt_marketactiontx
WHERE action IN (0,1)
),

markets as (
SELECT * FROM query_5971135
)

SELECT DATE_TRUNC('week', block_time) as "Week", COUNT(DISTINCT user) as "Unique Users"
FROM trades t
RIGHT JOIN markets m ON t.contract_address = m.contract_address AND t.marketId = m.marketId
GROUP BY 1
),

limitless as (
with amm as (
WITH markets as (
SELECT 
fixedProductMarketMaker AS market_address, collateralToken AS collateral
FROM limitless_base.FixedProductMarketMakerFactory_evt_FixedProductMarketMakerCreation
UNION ALL
SELECT
fixedProductMarketMaker AS market_address, collateralToken AS collateral
FROM limitless_exchange_prod_hanson_v0_1_base.FixedProductMarketMakerFactory_evt_FixedProductMarketMakerCreation
)

SELECT
DATE_TRUNC('week', block_time) as "Week", SUBSTR(topic1, 13, 20) 
FROM base.logs b 
INNER JOIN markets m ON b.contract_address = m.market_address
WHERE topic0 IN (0x4f62630f51608fc8a7603a9391a5101e58bd7c276139366fc107dc3b67c3dcf8, 0xadcf2a240ed9300d681d9a3f5382b6c1beed1b7e46643e0c7b42cbe6e2d766b4)
),

weekly as (
SELECT DATE_TRUNC('week', evt_block_time) as "Week", maker as "User"
FROM limitless_base.ctfexchange_evt_orderfilled -- single-outcome markets makers
UNION ALL
SELECT DATE_TRUNC('week', evt_block_time) as "Week", taker
FROM limitless_base.ctfexchange_evt_orderfilled -- single-outcome markets takers
UNION ALL
SELECT DATE_TRUNC('week', evt_block_time) as "Week", maker
FROM limitless_base.negriskctfexchange_evt_orderfilled -- multi-outcome markets makers
UNION ALL
SELECT DATE_TRUNC('week', evt_block_time) as "Week", taker
FROM limitless_base.negriskctfexchange_evt_orderfilled -- multi-outcome markets takers
UNION ALL
SELECT * FROM amm
)

SELECT "Week", COUNT(DISTINCT "User") as "Unique Users" FROM weekly
GROUP BY 1
),

polymarket as (
with amm as (
WITH markets as (
SELECT 
fixedProductMarketMaker AS market_address, collateralToken AS collateral
FROM polymarketfactory_polygon.FixedProductMarketMakerFactory_evt_FixedProductMarketMakerCreation
)

SELECT
DATE_TRUNC('week', block_time) as "Week", SUBSTR(topic1, 13, 20) 
FROM polygon.logs b 
INNER JOIN markets m ON b.contract_address = m.market_address
WHERE topic0 IN (0x4f62630f51608fc8a7603a9391a5101e58bd7c276139366fc107dc3b67c3dcf8, 0xadcf2a240ed9300d681d9a3f5382b6c1beed1b7e46643e0c7b42cbe6e2d766b4)
),

weekly as (
SELECT DATE_TRUNC('week', evt_block_time) as "Week", maker as "User"
FROM polymarket_polygon.ctfexchange_evt_orderfilled -- single-outcome markets makers
UNION ALL
SELECT DATE_TRUNC('week', evt_block_time) as "Week", taker
FROM polymarket_polygon.ctfexchange_evt_orderfilled -- single-outcome markets takers
UNION ALL
SELECT DATE_TRUNC('week', evt_block_time) as "Week", maker
FROM polymarket_polygon.negriskctfexchange_evt_orderfilled -- multi-outcome markets makers
UNION ALL
SELECT DATE_TRUNC('week', evt_block_time) as "Week", taker
FROM polymarket_polygon.negriskctfexchange_evt_orderfilled -- multi-outcome markets takers
UNION ALL
SELECT * FROM amm
)

SELECT "Week", COUNT(DISTINCT "User") as "Unique Users" FROM weekly
GROUP BY 1
),

opinion as (
WITH orderfills as (
SELECT *,
bytearray_ltrim(topic2) as maker,
bytearray_ltrim(topic3) as taker,
bytearray_to_uint256(bytearray_substring(data, 1, 32)) as makerAssetId,
bytearray_to_uint256(bytearray_substring(data, 33, 32)) as takerAssetId,
bytearray_to_uint256(bytearray_substring(data, 65, 32)) as makerAmountFilled,
bytearray_to_uint256(bytearray_substring(data, 97, 32)) as takerAmountFilled,
bytearray_to_uint256(bytearray_substring(data, 129, 32)) as fee
FROM bnb.logs
WHERE block_number >= 65733322
AND contract_address = 0x5F45344126D6488025B0b84A3A8189F2487a7246
AND topic0 = 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6
),

weekly as (
SELECT DATE_TRUNC('week', block_time) as "Week", maker as "User"
FROM orderfills 
UNION ALL
SELECT DATE_TRUNC('week', block_time) as "Week", taker
FROM orderfills
)

SELECT "Week", COUNT(DISTINCT "User") as "Unique Users" FROM weekly
GROUP BY 1
)

SELECT * FROM (
SELECT *, 'Myriad' as platform FROM myriad
UNION ALL
SELECT *, 'Limitless' as platform FROM limitless
UNION ALL
SELECT *, 'Polymarket' as platform FROM polymarket
UNION ALL
SELECT *, 'Opinion' as platform FROM opinion
)
WHERE week >= CAST('2024-04-08' AS DATE)
AND week < DATE_TRUNC('week', NOW())
'''