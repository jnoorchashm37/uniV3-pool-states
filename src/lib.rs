use alloy_primitives::Address;
use db::{spawn_clickhouse_db, UniswapV3Tables};
use db_interfaces::clickhouse::client::ClickhouseClient;
use db_interfaces::Database;
use handler::PoolHandler;
use node::RethDbApiClient;
use pools::TickFetcher;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Handle;
use tracing::Level;

pub mod aux;
pub mod db;
pub mod handler;
pub mod node;
pub mod pools;

pub async fn run(handle: Handle) -> eyre::Result<()> {
    aux::init(vec![aux::stdout(
        format!("uni-v3={}", Level::INFO).parse()?,
    )]);

    init_threadpool();

    let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
    let node = Arc::new(RethDbApiClient::new(&reth_db_path, handle.clone()).await?);

    let db = Box::leak(Box::new(spawn_clickhouse_db()));

    let current_block = node.get_current_block()?;

    let (min_block, pools) = get_initial_pools(db).await?;

    let handler = PoolHandler::new(
        node,
        db,
        Box::leak(Box::new(pools)),
        min_block,
        current_block,
        handle.clone(),
        1000,
    );

    //   let handler = PoolHandler::new(node, db, pools, 12369821, 12369825, handle.clone(), 100);

    handler.await;

    Ok(())
}

async fn get_initial_pools(
    db: &'static ClickhouseClient<UniswapV3Tables>,
) -> eyre::Result<(u64, Vec<TickFetcher>)> {
    let query = "
        SELECT DISTINCT
            (toString(address), init_block)
        FROM ethereum.pools
        WHERE address = '0x4e68ccd3e89f51c3074ca5072bbac773960dfa36' OR 
        address = '0xcbcdf9626bc03e24f779434178a73a0b4bad62ed' OR 
        address = '0x11b815efb8f581194ae79006d24e0d814b7697f6' OR 
        address = '0xc63b0708e2f7e69cb8a1df0e1389a98c35a76d52' OR 
        address = '0x99ac8ca7087fa4a2a1fb6357269965a2014abc35' OR 
        address = '0x7a415b19932c0105c82fdb6b720bb01b0cc2cae3' OR 
        address = '0x5777d92f208679db4b9778590fa3cab3ac9e2168' OR 
        address = '0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8' OR 
        address = '0xa6cc3c2531fdaa6ae1a3ca84c2855806728693e8' OR 
        address = '0x11950d141ecb863f01007add7d1a342041227b58' OR 
        address = '0x9db9e0e53058c89e5b94e29621a205198648425b' OR 
        address = '0xe8c6c9227491c0a8156a0106a0204d881bb7e531'
    ";

    //AND address = '0xcbcdf9626bc03e24f779434178a73a0b4bad62ed'

    let initial_pools: Vec<(String, u64)> = db.query_many(query, &()).await?;

    let pools = initial_pools
        .into_iter()
        .map(|(addr, blk)| TickFetcher::new(Address::from_str(&addr).unwrap(), blk))
        .collect::<Vec<_>>();

    let min_block = pools.iter().map(|p| p.earliest_block).min().unwrap();

    Ok((min_block, pools))
}

static RAYON_PRICING_THREADPOOL: OnceLock<rayon::ThreadPool> = OnceLock::new();

pub fn init_threadpool() {
    let threadpool = rayon::ThreadPoolBuilder::new()
        .num_threads(256)
        .build()
        .unwrap();

    let _ = RAYON_PRICING_THREADPOOL.set(threadpool);
}

pub fn execute_on_threadpool<OP, R>(op: OP) -> R
where
    OP: FnOnce() -> R + Send,
    R: Send,
{
    RAYON_PRICING_THREADPOOL
        .get()
        .expect("threadpool not initialized")
        .install(op)
}
