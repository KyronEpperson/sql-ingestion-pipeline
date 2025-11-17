from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5749464
table_name = "Prediction_Market_Open_Interest"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
with myriad as (
WITH flows AS (
  SELECT
    DATE_TRUNC('day', e.evt_block_time) AS day,
    e.contract_address,
    SUM(
      CASE
        WHEN e."to"   IN (0x3e0F5F8F5Fb043aBFA475C0308417Bf72c463289, 0x4f4988A910f8aE9B3214149A8eA1F2E4e3Cd93CC) THEN e."value"
        WHEN e."from" IN (0x3e0F5F8F5Fb043aBFA475C0308417Bf72c463289, 0x4f4988A910f8aE9B3214149A8eA1F2E4e3Cd93CC) THEN -e."value"
        ELSE 0
      END
    ) AS tokens
  FROM erc20_abstract.evt_transfer e
  WHERE e."to"   IN (0x3e0F5F8F5Fb043aBFA475C0308417Bf72c463289, 0x4f4988A910f8aE9B3214149A8eA1F2E4e3Cd93CC)
     OR e."from" IN (0x3e0F5F8F5Fb043aBFA475C0308417Bf72c463289, 0x4f4988A910f8aE9B3214149A8eA1F2E4e3Cd93CC)
  GROUP BY 1, 2
  UNION ALL
  SELECT
    DATE_TRUNC('day', e.evt_block_time) AS day,
    e.contract_address,
    SUM(
      CASE
        WHEN e."to"   IN (0x39e66ee6b2ddaf4defded3038e0162180dbef340) THEN e."value"
        WHEN e."from" IN (0x39e66ee6b2ddaf4defded3038e0162180dbef340) THEN -e."value"
        ELSE 0
      END
    ) AS tokens
  FROM erc20_linea.evt_transfer e
  WHERE e."to"   IN (0x39e66ee6b2ddaf4defded3038e0162180dbef340)
     OR e."from" IN (0x39e66ee6b2ddaf4defded3038e0162180dbef340)
  GROUP BY 1, 2
  UNION ALL
  SELECT
    DATE_TRUNC('day', e.evt_block_time) AS day,
    e.contract_address,
    SUM(
      CASE
        WHEN e."to"   IN (0x39e66ee6b2ddaf4defded3038e0162180dbef340) THEN e."value"
        WHEN e."from" IN (0x39e66ee6b2ddaf4defded3038e0162180dbef340) THEN -e."value"
        ELSE 0
      END
    ) AS tokens
  FROM erc20_bnb.evt_transfer e
  WHERE e."to"   IN (0x39e66ee6b2ddaf4defded3038e0162180dbef340)
     OR e."from" IN (0x39e66ee6b2ddaf4defded3038e0162180dbef340)
  GROUP BY 1, 2
),

-- One row per contract with its decimals
token_meta AS (
  SELECT
    contract_address,
    MAX(decimals) AS decimals
  FROM tokens.erc20
  GROUP BY 1
),

daily_prices AS (
  SELECT
    DATE_TRUNC('day', p.minute) AS day,
    p.contract_address,
    AVG(p.price) AS price
  FROM prices.usd p
  GROUP BY 1, 2
),

daily_usd_change AS (
  SELECT
    f.day,
    SUM(
      (f.tokens / POWER(10, COALESCE(t.decimals, 18)))
      * COALESCE(dp.price, 0)
    ) AS tvl_change_usd
  FROM flows f
  LEFT JOIN token_meta   t  ON t.contract_address = f.contract_address
  LEFT JOIN daily_prices dp ON dp.contract_address = f.contract_address
                            AND dp.day = f.day
  GROUP BY 1
)

SELECT * FROM (
SELECT
  day,
  tvl_change_usd AS "TVL Change",
  SUM(tvl_change_usd) OVER (
    ORDER BY day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS "TVL"
FROM daily_usd_change
)
),

limitless as (
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
SELECT date_trunc('day', minute) as day, contract_address, decimals, AVG(price) as price
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
),

Raw as (
SELECT DATE_TRUNC('day', evt_block_time) as day, SUM(value / POW(10, COALESCE(c.decimals, d.decimals))*COALESCE(c.price, d.price, 1000000)) as Value FROM erc20_base.evt_transfer e
LEFT JOIN collaterals c ON e.contract_address = c.contract_address AND DATE_TRUNC('day', e.evt_block_time) = c.day
LEFT JOIN dexprices d ON e.contract_address = d.address AND DATE_TRUNC('day', e.evt_block_time) = d.block_date
WHERE "to" IN (0xC9c98965297Bc527861c898329Ee280632B76e18, 0x5d6C6a4fEA600E0b1A3Ab3eF711060310E27886A)
GROUP BY 1
UNION ALL
SELECT DATE_TRUNC('day', evt_block_time) as day, -SUM(value / POW(10, COALESCE(c.decimals, d.decimals))*COALESCE(c.price, d.price, 1000000)) as Value FROM erc20_base.evt_transfer e
LEFT JOIN collaterals c ON e.contract_address = c.contract_address AND DATE_TRUNC('day', e.evt_block_time) = c.day
LEFT JOIN dexprices d ON e.contract_address = d.address AND DATE_TRUNC('day', e.evt_block_time) = d.block_date
WHERE "from" IN (0xC9c98965297Bc527861c898329Ee280632B76e18, 0x5d6C6a4fEA600E0b1A3Ab3eF711060310E27886A)
GROUP BY 1
),

Cumulative as (
SELECT day, SUM(Value) OVER (ORDER BY day) as TVL FROM Raw 
)

SELECT c.*, SUM(r.Value) as TVLDelta FROM cumulative c
INNER JOIN raw r ON c.day = r.day
GROUP BY 1,2
),

kalshi as (
SELECT date, SUM(value) as open_interest FROM dune.datadashboardsapi.dataset_kalshi_daily_metrics
WHERE "Type" = 'Open Interest'
GROUP BY 1
),

polymarket as (
WITH raw as (
SELECT DATE_TRUNC('day', evt_block_time) as day, SUM(Value/1E6) as Value FROM erc20_polygon.evt_transfer
WHERE "to" IN (0x4D97DCd97eC945f40cF65F87097ACe5EA0476045, 0x3A3BD7bb9528E159577F7C2e685CC81A765002E2)
AND contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174
GROUP BY 1
UNION ALL
SELECT DATE_TRUNC('day', evt_block_time) as day, -SUM(Value/1E6) as Value FROM erc20_polygon.evt_transfer
WHERE "from" IN (0x4D97DCd97eC945f40cF65F87097ACe5EA0476045, 0x3A3BD7bb9528E159577F7C2e685CC81A765002E2)
AND contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174
GROUP BY 1
)

SELECT day, SUM(Value) as TVLDelta, SUM(SUM(Value)) OVER (ORDER BY day) as "TVL" FROM raw 
GROUP BY 1
),

opinion as (
WITH raw as (
SELECT DATE_TRUNC('day', evt_block_time) as day, SUM(Value/1E18) as Value FROM erc20_bnb.evt_transfer
WHERE "to" IN (0xad1a38cec043e70e83a3ec30443db285ed10d774)
AND contract_address = 0x55d398326f99059fF775485246999027B3197955
AND evt_block_number >= 64726315
GROUP BY 1
UNION ALL
SELECT DATE_TRUNC('day', evt_block_time) as day, -SUM(Value/1E18) as Value FROM erc20_bnb.evt_transfer
WHERE "from" IN (0xad1a38cec043e70e83a3ec30443db285ed10d774)
AND contract_address = 0x55d398326f99059fF775485246999027B3197955
AND evt_block_number >= 64726315
GROUP BY 1
)

SELECT day, SUM(Value) as TVLDelta, SUM(SUM(Value)) OVER (ORDER BY day) as "TVL" FROM raw 
GROUP BY 1
)

SELECT * FROM (
SELECT day, "TVL", 'Myriad' as platform FROM myriad
UNION ALL
SELECT day, "TVL", 'Limitless' as platform FROM limitless
UNION ALL
SELECT CAST(date AS DATE), open_interest, 'Kalshi' as platform FROM kalshi
UNION ALL
SELECT day, "TVL", 'Polymarket' as platform FROM polymarket
UNION ALL
SELECT day, "TVL", 'Opinion' as platform FROM opinion
)
WHERE day >= CAST('2024-04-08' AS DATE)
AND day < DATE_TRUNC('day', NOW())

'''