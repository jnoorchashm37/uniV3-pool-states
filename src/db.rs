use db_interfaces::{
    clickhouse::client::ClickhouseClient, clickhouse_dbms, remote_clickhouse_table,
};

use crate::pools::PoolState;

clickhouse_dbms!(UniswapV3Tables, [UniV3PoolState]);

remote_clickhouse_table!(
    UniswapV3Tables,
    "eth_analytics",
    UniV3PoolState,
    PoolState,
    "src/pools/sql/"
);

pub fn spawn_clickhouse_db() -> ClickhouseClient<UniswapV3Tables> {
    ClickhouseClient::<UniswapV3Tables>::default()
}
