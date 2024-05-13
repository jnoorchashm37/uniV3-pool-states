use alloy_primitives::Address;
use db::{spawn_clickhouse_db, UniswapV3Tables};
use db_interfaces::clickhouse::client::ClickhouseClient;
use db_interfaces::Database;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use handler::PoolHandler;
use node::RethDbApiClient;
use std::str::FromStr;
use std::sync::Arc;
use ticks::TickFetcher;
use tokio::runtime::Handle;
use tracing::Level;

pub mod aux;
pub mod contracts;
pub mod db;
pub mod handler;
pub mod node;
pub mod state;
pub mod ticks;

pub async fn run(handle: Handle) -> eyre::Result<()> {
    aux::init(vec![aux::stdout(Level::INFO.into())]);

    let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
    let node = Arc::new(RethDbApiClient::new(&reth_db_path, handle).await?);

    let db = Arc::new(spawn_clickhouse_db());

    let current_block = node.get_current_block()?;

    let pools = get_initial_pools(node, db).await?;

    let handlers = pools
        .into_iter()
        .map(|p| PoolHandler::new(p, current_block, 10))
        .collect::<FuturesUnordered<_>>();

    join_all(handlers).await;

    Ok(())
}

async fn get_initial_pools(
    node: Arc<RethDbApiClient>,
    db: Arc<ClickhouseClient<UniswapV3Tables>>,
) -> eyre::Result<Vec<TickFetcher>> {
    let query = "
        SELECT DISTINCT
            (toString(address), init_block)
        FROM ethereum.pools
        WHERE protocol = 'Uniswap' AND protocol_subtype = 'V3' AND address = '0xcbcdf9626bc03e24f779434178a73a0b4bad62ed'
    ";

    let initial_pools: Vec<(String, u64)> = db.query_many(query, &()).await?;

    join_all(initial_pools.into_iter().map(|(addr, blk)| {
        TickFetcher::new(
            node.clone(),
            db.clone(),
            Address::from_str(&addr).unwrap(),
            blk,
        )
    }))
    .await
    .into_iter()
    .collect()
}
