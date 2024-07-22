CREATE TABLE eth_analytics.uni_v3_trades ON CLUSTER eth_cluster0
(
    `block_number` UInt64,
    `tx_hash` String,
    `pool_address` String,
    `token_in` String,
    `token_in_decimals` UInt8,
    `token_in_amount` Int256,
    `token_out` String,
    `token_out_decimals` UInt8,
    `token_out_amount` Int256,
    `calculated_price` Float64,
    `last_updated` UInt64 Default now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/eth_cluster0/tables/all/eth_analytics/uni_v3_trades', '{replica}', `last_updated`)
PRIMARY KEY (`block_number`, `pool_address`)
ORDER BY (`block_number`, `pool_address`, `tx_hash`)