from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5741393
table_name = "Weekly_Prediction_Market_Volume"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
with myriad as (
with trades as (
SELECT evt_block_time, evt_tx_hash, contract_address, action, marketId, value, 'abstract' as chain  FROM myriad_abstract.predictionmarketv3_evt_marketactiontx
WHERE action IN (0,1)
UNION ALL 
SELECT evt_block_time, evt_tx_hash, contract_address, action, marketId, value, chain FROM myriad_multichain.predictionmarketv3_4_evt_marketactiontx
WHERE action IN (0,1)
UNION ALL 
SELECT evt_block_time, evt_tx_hash, contract_address, action, marketId, value, 'abstract' as chain  FROM myriad_abstract.predictionmarketv3_3_points_evt_marketactiontx
WHERE action IN (0,1)
UNION ALL 
SELECT evt_block_time, evt_tx_hash, contract_address, action, marketId, value, 'abstract' as chain  FROM myriad_abstract.predictionmarketv4_evt_marketactiontx
WHERE action IN (0,1)
),

markets as (
SELECT * FROM query_5971135
)

SELECT date_trunc('week', evt_block_time) as Week, 
SUM(value*price/POW(10, decimals)) as "USD Volume"
FROM trades t
RIGHT JOIN markets m ON t.contract_address = m.contract_address AND t.marketId = m.marketId
LEFT JOIN prices.usd p ON m.token = CAST(p.contract_address AS VARCHAR) AND t.chain = p.blockchain AND DATE_TRUNC('minute', t.evt_block_time) = p.minute
WHERE p.blockchain IN ('abstract', 'linea', 'bnb')
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
),

collaterals AS (
SELECT minute, contract_address, decimals, AVG(price) as price
FROM prices.usd
WHERE blockchain = 'base'
and contract_address IN (SELECT collateral FROM markets) 
GROUP BY 1,2,3
),

dexprices as (
SELECT block_date, AVG(price) as price, address, 
CASE 
WHEN address = 0xcd2f22236dd9dfe2356d7c543161d4d260fd9bcb THEN 18
WHEN address = 0x490a4b510d0ea9f835d2df29eb73b4fca5071937 THEN 18
WHEN address = 0xfef2d7b013b88fec2bfe4d2fee0aeb719af73481 THEN 18
WHEN address = 0x4e65fe4dba92790696d040ac24aa414708f5c0ab THEN 6
END AS decimals
FROM (
SELECT block_date, amount_usd/token_bought_amount as price, token_bought_address as address FROM dex.trades
WHERE token_bought_address IN (0xfef2d7b013b88fec2bfe4d2fee0aeb719af73481, 0x4e65fe4dba92790696d040ac24aa414708f5c0ab, 0xcd2f22236dd9dfe2356d7c543161d4d260fd9bcb, 0x490a4b510d0ea9f835d2df29eb73b4fca5071937)
AND blockchain = 'base'
AND amount_usd IS NOT NULL
UNION ALL
SELECT block_date, amount_usd/token_sold_amount as price, token_sold_address as address FROM dex.trades
WHERE token_sold_address IN (0xfef2d7b013b88fec2bfe4d2fee0aeb719af73481, 0x4e65fe4dba92790696d040ac24aa414708f5c0ab, 0xcd2f22236dd9dfe2356d7c543161d4d260fd9bcb, 0x490a4b510d0ea9f835d2df29eb73b4fca5071937)
AND blockchain = 'base'
AND amount_usd IS NOT NULL
)
GROUP BY 1,3
)

SELECT
DATE_TRUNC('week', block_time) as "Week",
SUM(bytearray_to_uint256(substr(DATA, 1, 32)) / POW(10, COALESCE(c.decimals, d.decimals))*COALESCE(c.price, d.price, 1000000)) as Volume
FROM base.logs b 
INNER JOIN markets m ON b.contract_address = m.market_address
LEFT JOIN collaterals c ON m.collateral = c.contract_address AND DATE_TRUNC('minute', b.block_time) = c.minute
LEFT JOIN dexprices d ON m.collateral = d.address AND DATE_TRUNC('day', b.block_time) = d.block_date
WHERE topic0 IN (0x4f62630f51608fc8a7603a9391a5101e58bd7c276139366fc107dc3b67c3dcf8, 0xadcf2a240ed9300d681d9a3f5382b6c1beed1b7e46643e0c7b42cbe6e2d766b4)
GROUP BY 1
)

SELECT "Week", SUM("Volume") as "Volume" FROM (
SELECT DATE_TRUNC('week', evt_block_time) as "Week",
CASE
WHEN CAST(makerAssetId AS VARCHAR) = '0' THEN CAST(makerAmountFilled AS DOUBLE) / 1e6
WHEN CAST(takerAssetId AS VARCHAR) = '0' THEN CAST(takerAmountFilled AS DOUBLE) / 1e6
END AS "Volume"
FROM limitless_base.ctfexchange_evt_orderfilled
UNION ALL
SELECT DATE_TRUNC('week', evt_block_time) as "Week", 
CASE
WHEN CAST(makerAssetId AS VARCHAR) = '0' THEN CAST(makerAmountFilled AS DOUBLE) / 1e6
WHEN CAST(takerAssetId AS VARCHAR) = '0' THEN CAST(takerAmountFilled AS DOUBLE) / 1e6
END AS "Volume"
FROM limitless_base.negriskctfexchange_evt_orderfilled
UNION ALL
SELECT * FROM amm
)
GROUP BY 1
),

polymarket as (
with amm as (
WITH markets as (
SELECT 
fixedProductMarketMaker AS market_address, collateralToken AS collateral
FROM polymarketfactory_polygon.fixedproductmarketmakerfactory_evt_fixedproductmarketmakercreation
),

collaterals AS (
SELECT minute, contract_address, decimals, AVG(price) as price
FROM prices.usd
WHERE blockchain = 'polygon'
and contract_address IN (SELECT collateral FROM markets) 
GROUP BY 1,2,3
)

SELECT
DATE_TRUNC('week', block_time) as "week",
SUM(bytearray_to_uint256(substr(DATA, 1, 32)) / POW(10, c.decimals*c.price)) as Volume
FROM polygon.logs b 
INNER JOIN markets m ON b.contract_address = m.market_address
LEFT JOIN collaterals c ON m.collateral = c.contract_address AND DATE_TRUNC('minute', b.block_time) = c.minute
WHERE topic0 IN (0x4f62630f51608fc8a7603a9391a5101e58bd7c276139366fc107dc3b67c3dcf8, 0xadcf2a240ed9300d681d9a3f5382b6c1beed1b7e46643e0c7b42cbe6e2d766b4)
GROUP BY 1
)

SELECT "week", SUM("Volume") as "Volume" FROM (
SELECT DATE_TRUNC('week', evt_block_time) as "week",
CASE
WHEN CAST(makerAssetId AS VARCHAR) = '0' THEN CAST(makerAmountFilled AS DOUBLE) / 1e6
WHEN CAST(takerAssetId AS VARCHAR) = '0' THEN CAST(takerAmountFilled AS DOUBLE) / 1e6
END AS "Volume"
FROM polymarket_polygon.ctfexchange_evt_orderfilled
UNION ALL
SELECT DATE_TRUNC('week', evt_block_time) as "week", 
CASE
WHEN CAST(makerAssetId AS VARCHAR) = '0' THEN CAST(makerAmountFilled AS DOUBLE) / 1e6
WHEN CAST(takerAssetId AS VARCHAR) = '0' THEN CAST(takerAmountFilled AS DOUBLE) / 1e6
END AS "Volume"
FROM polymarket_polygon.negriskctfexchange_evt_orderfilled
UNION ALL
SELECT * FROM amm
)
GROUP BY 1
),

opinion as (
WITH orderfills as (
SELECT *,
bytearray_to_uint256(bytearray_substring(data, 1, 32)) as makerAssetId,
bytearray_to_uint256(bytearray_substring(data, 33, 32)) as takerAssetId,
bytearray_to_uint256(bytearray_substring(data, 65, 32)) as makerAmountFilled,
bytearray_to_uint256(bytearray_substring(data, 97, 32)) as takerAmountFilled,
bytearray_to_uint256(bytearray_substring(data, 129, 32)) as fee
FROM bnb.logs
WHERE block_number >= 65733322
AND contract_address = 0x5F45344126D6488025B0b84A3A8189F2487a7246
AND topic0 = 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6
)

SELECT "Week", SUM("Volume") as "Volume" FROM (
SELECT DATE_TRUNC('week', block_time) as "Week",
CASE
WHEN CAST(makerAssetId AS VARCHAR) = '0' THEN CAST(makerAmountFilled AS DOUBLE) / 1e18
WHEN CAST(takerAssetId AS VARCHAR) = '0' THEN CAST(takerAmountFilled AS DOUBLE) / 1e18
END AS "Volume"
FROM orderfills
)
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