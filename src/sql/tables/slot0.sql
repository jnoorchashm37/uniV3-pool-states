CREATE TABLE eth_analytics.uni_v3_slot0 ON CLUSTER eth_cluster0
(
    `block_number` UInt64,
    `pool_address` String,
    `token0` String,
    `token0_decimals` UInt8,
    `token1` String,
    `token1_decimals` UInt8,
    `tx_hash` String,
    `tx_index` UInt64,
    `tick` Int32,
    `sqrt_price_x96` UInt256,
    `calculated_price` Float64,
    `observation_index` UInt16,
    `observation_cardinality` UInt16,
    `observation_cardinality_next` UInt16,
    `fee_protocol` UInt8,
    `unlocked`  Bool,
    `last_updated` UInt64 Default now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/eth_cluster0/tables/all/eth_analytics/uni_v3_slot0', '{replica}', `last_updated`)
PRIMARY KEY (`block_number`, `pool_address`)
ORDER BY (`block_number`, `pool_address`, `tx_hash`, `tick`)
