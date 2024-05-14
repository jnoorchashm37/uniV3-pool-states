CREATE TABLE eth_analytics.uni_v3_pool_state ON CLUSTER eth_cluster0
(
    `block_number` UInt64,
    `tx_hash` String,
    `pool_address` String,
    `tick` Int32,
    `tick_spacing` Int32,
    `liquidity_gross` UInt128,
    `liquidity_net` Int128,
    `fee_growth_outside_0_x128` UInt256,
    `fee_growth_outside_1_x128` UInt256,
    `tick_cumulative_outside` Int64,
    `seconds_per_liquidity_outside_x128`  UInt256,
    `seconds_outside` UInt32,
    `initialized` Bool,
    `last_updated` UInt64 Default now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/eth_cluster0/tables/all/eth_analytics/uni_v3_pool_state', '{replica}', `last_updated`)
PRIMARY KEY (`block_number`, `pool_address`)
ORDER BY (`block_number`, `pool_address`, `tx_hash`, `tick`)


SELECT 
    block_number,
    pool_address,
    tick,
    groupUniqArray(tx_hash),
    groupUniqArray((liquidity_gross, liquidity_net, fee_growth_outside_0_x128, fee_growth_outside_1_x128)) AS list
FROM eth_analytics.uni_v3_pool_state
WHERE (block_number, pool_address, tick) IN (
    SELECT
        block_number,
        pool_address,
        tick
    FROM eth_analytics.uni_v3_pool_state
    WHERE block_number >= 19861259 AND block_number <= 19867068
    GROUP BY
        block_number,
        pool_address,
        tick
    HAVING countDistinct(tx_hash) > 1
)-- AND block_number = 19862732 AND pool_address = '0x11b815efb8f581194ae79006d24e0d814b7697f6'
GROUP BY
    block_number,
    pool_address,
    tick
HAVING length(list) > 1
ORDER BY tick
