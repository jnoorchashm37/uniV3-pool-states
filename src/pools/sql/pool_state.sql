CREATE TABLE eth_analytics.uni_v3_pool_state ON CLUSTER eth_cluster0
(
    `block_number` UInt64,
    `pool_address` String,
    `tx_hash` String,
    `tx_index` UInt64,
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