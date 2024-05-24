CREATE TABLE default.karthik_dex ON CLUSTER eth_cluster0
(
    `block_number` UInt64,
    `block_timestamp` DateTime,
    `pool_address` String,
    `price` Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/eth_cluster0/tables/all/default/karthik_dex', '{replica}')
PRIMARY KEY (block_number, pool_address)
ORDER BY (block_number, pool_address)





CREATE TABLE default.karthik_dex ON CLUSTER eth_cluster0 (
    `block_number` UInt64,
    `block_timestamp` DateTime,
    `pool_address` String,
    `price` Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/eth_cluster0/tables/all/default/karthik_dex', '{replica}')
PRIMARY KEY (`block_number`, `pool_address`)
ORDER BY (`block_number`, `pool_address`)


CREATE TABLE default.karthik_cex ON CLUSTER eth_cluster0
(
    `symbol` String,
    `quote_timestamp` DateTime64(3),
    `price` Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/eth_cluster0/tables/all/default/karthik_cex', '{replica}')
PRIMARY KEY `symbol`
ORDER BY `symbol`






SELECT
    timestamp,
    (avg(ask_price) + avg(ask_price))/2 AS price
FROM cex.normalized_quotes
WHERE 
    timestamp >= 1702746431 * 1000000 AND timestamp < 1702747031 * 1000000 
    AND symbol LIKE 'ETH%' AND (symbol LIKE '%USDC' OR symbol LIKE '%USDT')
GROUP BY timestamp



SELECT
    distinct 
    symbol 
    FROM cex.normalized_quotes WHERE 
timestamp >= 1702746431 * 1000000 AND timestamp < 1702747031 * 1000000 
AND symbol LIKE 'ETH%' AND (symbol LIKE '%USDC' OR symbol LIKE '%USDT')





INSERT INTO default.karthik_cex
SELECT
    'ETH-USD' AS symbol,
    toDateTime64(timestamp/1000000.0, 6) AS quote_timestamp,
    (avg(ask_price) + avg(ask_price))/2 AS price
FROM cex.normalized_quotes
WHERE 
    exchange = 'binance' AND
    timestamp >= 1709745839 * 1000000 AND timestamp <= 1709746439 * 1000000 
    AND normalized_quotes.symbol LIKE 'ETH%' AND (normalized_quotes.symbol LIKE '%USDC' OR normalized_quotes.symbol LIKE '%USDT')
GROUP BY timestamp
ORDER BY timestamp DESC
SETTINGS parallel_distributed_insert_select=0