use clap::Parser;
use cli::CliCmd;
use db::{get_initial_pools, spawn_clickhouse_db};
use node::EthNodeApi;
use pools::{PoolFetcher, PoolSlot0, PoolSlot0Fetcher, PoolTickFetcher};
use std::sync::{Arc, OnceLock};
use tokio::{runtime::Handle, sync::mpsc::unbounded_channel};
use tracing::{info, Level};
use utils::TokenInfo;

mod handler;
pub use handler::*;

mod runner;
pub use runner::*;

use crate::db::BufferedClickhouse;

mod aux;
pub use aux::{execute_on_threadpool, init_all};
pub mod db;

mod cli;

pub mod const_sql;

pub mod node;
pub mod pools;
pub mod utils;

pub fn run() -> eyre::Result<()> {
    runner::run_command_until_exit(|ctx| execute(ctx.task_executor))?;

    Ok(())
}

async fn execute(executor: TaskExecutor) -> eyre::Result<()> {
    let cli = CliCmd::parse();
    aux::init_all(cli.verbosity.directive());

    let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
    let node = Arc::new(EthNodeApi::new(&reth_db_path, executor.handle().clone())?);
    let current_block = node.get_current_block()?;

    let db = Arc::new(spawn_clickhouse_db());

    let (tx, rx) = unbounded_channel();
    let buffered_db = BufferedClickhouse::new(db.clone(), rx, 100000);
    executor.spawn_blocking(buffered_db);

    let (min_block, pools) = get_initial_pools(&db).await?;
    info!(target: "uniV3", "starting block range {min_block} - {current_block} for {} pools",pools.len());

    let mut pool_fetchers = Vec::new();
    if cli.slot0 {
        let slot0_pools = pools.iter().map(|pool| {
            Arc::new(Box::new(PoolSlot0Fetcher::new(
                pool.pool_address,
                TokenInfo::new(pool.token0_address, pool.token0_decimals),
                TokenInfo::new(pool.token1_address, pool.token1_decimals),
                pool.creation_block,
            )) as Box<dyn PoolFetcher>)
        });
        pool_fetchers.extend(slot0_pools)
    }

    if cli.tick_info {
        let tick_info_pools = pools.iter().map(|pool| {
            Arc::new(
                Box::new(PoolTickFetcher::new(pool.pool_address, pool.creation_block))
                    as Box<dyn PoolFetcher>,
            )
        });
        pool_fetchers.extend(tick_info_pools)
    }

    // let this_handle = handle.clone();
    // let buffered_db_handle = handle
    //     .clone()
    //     .spawn_blocking(move || this_handle.block_on());

    let handler = PoolHandler::new(
        node,
        tx.clone(),
        pool_fetchers,
        cli.start_block.unwrap_or(min_block),
        cli.end_block.unwrap_or(current_block),
        executor.handle().clone(),
    );

    executor
        .spawn_critical_blocking("uniV3 pool executor", handler)
        .await?;

    Ok(())
}
