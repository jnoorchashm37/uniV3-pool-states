use db::{get_initial_pools, spawn_clickhouse_db};
use handler::PoolHandler;
use node::RethDbApiClient;
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
