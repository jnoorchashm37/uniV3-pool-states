use clap::Parser;
use cli::CliCmd;
use db::{get_initial_pools, spawn_clickhouse_db};
use node::EthNodeApi;
use pools::{PoolFetcher, PoolSlot0Fetcher, PoolTickFetcher, PoolTradeFetcher};
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tracing::info;
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
    let buffered_db = BufferedClickhouse::new(db.clone(), rx, cli.insert_size);
    executor.spawn_blocking(buffered_db);

    let (min_block, pools) = get_initial_pools(&db).await?;

    let mut pool_fetchers = Vec::new();
    if cli.slot0 {
        info!(target: "uniV3::slot0", "enabled slot0 fetcher");
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
        info!(target: "uniV3::tick-info", "enabled tick-info fetcher");
        let tick_info_pools = pools.iter().map(|pool| {
            Arc::new(
                Box::new(PoolTickFetcher::new(pool.pool_address, pool.creation_block))
                    as Box<dyn PoolFetcher>,
            )
        });
        pool_fetchers.extend(tick_info_pools)
    }

    if cli.trades {
        info!(target: "uniV3::trades", "enabled trades fetcher");
        let trade_pools = pools.iter().map(|pool| {
            Arc::new(Box::new(PoolTradeFetcher::new(
                pool.pool_address,
                TokenInfo::new(pool.token0_address, pool.token0_decimals),
                TokenInfo::new(pool.token1_address, pool.token1_decimals),
                pool.creation_block,
            )) as Box<dyn PoolFetcher>)
        });
        pool_fetchers.extend(trade_pools)
    }

    let start_block = cli.start_block.unwrap_or(min_block);
    let end_block = cli.end_block.unwrap_or(current_block);
    info!(target: "uniV3", "starting block range {start_block} - {end_block} for {} pools", pools.len());

    let handler = PoolHandler::new(
        node,
        tx.clone(),
        pool_fetchers,
        start_block,
        end_block,
        executor.handle().clone(),
        cli.max_concurrent_tasks,
    );

    executor
        .spawn_critical_blocking("uniV3 pool executor", handler)
        .await?;

    Ok(())
}
