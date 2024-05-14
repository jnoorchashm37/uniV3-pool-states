use db::{get_initial_pools, spawn_clickhouse_db};
use handler::PoolHandler;
use node::RethDbApiClient;
use std::sync::{Arc, OnceLock};
use tokio::{runtime::Handle, sync::mpsc::unbounded_channel};
use tracing::{info, Level};

use crate::db::BufferedClickhouse;

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
    let current_block = node.get_current_block()?;

    let db = spawn_clickhouse_db();
    let (min_block, pools) = get_initial_pools(&db).await?;

    info!(target: "uni-v3", "starting block range {min_block} - {current_block} for {} pools",pools.len());

    let (tx, rx) = unbounded_channel();
    let buffered_db = BufferedClickhouse::new(db, rx, 100000);

    let this_handle = handle.clone();
    handle
        .clone()
        .spawn_blocking(|| this_handle.block_on(buffered_db));

    let handler = PoolHandler::new(
        node,
        tx,
        Box::leak(Box::new(pools)),
        min_block,
        current_block,
        handle.clone(),
    );

    handler.await;

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
