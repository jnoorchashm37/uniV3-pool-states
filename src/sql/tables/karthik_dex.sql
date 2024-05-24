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
