use db::{get_initial_pools, spawn_clickhouse_db};
use node::RethDbApiClient;
use pools::PoolHandler;
use std::sync::{Arc, OnceLock};
use tokio::{runtime::Handle, sync::mpsc::unbounded_channel};
use tracing::{info, Level};

use crate::db::BufferedClickhouse;

pub mod aux;
pub mod db;

pub mod node;
pub mod pools;

pub async fn run(handle: Handle) -> eyre::Result<()> {
    aux::init(vec![aux::stdout(
        format!("uni-v3={}", Level::INFO).parse()?,
    )]);

    init_threadpool();
    info!(target: "uni-v3", "initialized rayon threadpool");

    let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
    let node = Arc::new(RethDbApiClient::new(&reth_db_path, handle.clone()).await?);
    info!(target: "uni-v3", "spawned eth node connection");

    let current_block = node.get_current_block()?;

    let db = Arc::new(spawn_clickhouse_db());
    info!(target: "uni-v3", "started clickhouse db connection");

    let (min_block, pools) = get_initial_pools(&db).await?;
    info!(target: "uni-v3", "starting block range {min_block} - {current_block} for {} pools",pools.len());

    let (tx, rx) = unbounded_channel();
    let buffered_db = BufferedClickhouse::new(db, rx, 100000);
    info!(target: "uni-v3", "created buffered clickhouse connection");

    let this_handle = handle.clone();
    let buffered_db_handle = handle
        .clone()
        .spawn_blocking(move || this_handle.block_on(buffered_db));

    let handler = PoolHandler::new(
        node,
        tx.clone(),
        Box::leak(Box::new(pools)),
        12370243,
        12370245,
        handle.clone(),
    );

    handler.await;

    drop(tx);
    buffered_db_handle.await?;

    Ok(())
}

static RAYON_PRICING_THREADPOOL: OnceLock<rayon::ThreadPool> = OnceLock::new();

pub fn init_threadpool() {
    let threadpool = rayon::ThreadPoolBuilder::new().build().unwrap();

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
