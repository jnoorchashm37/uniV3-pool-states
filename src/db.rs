use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use alloy_primitives::Address;
use clickhouse::Row;
use db_interfaces::{
    clickhouse::{client::ClickhouseClient, config::ClickhouseConfig},
    clickhouse_dbms, remote_clickhouse_table, Database,
};
use futures::{Future, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info};

use crate::{
    const_sql::INITIAL_POOLS,
    pools::{PoolData, PoolSlot0, PoolTickInfo},
    utils::serde_address,
};

clickhouse_dbms!(UniswapV3Tables, [UniV3TickInfo, UniV3Slot0]);

remote_clickhouse_table!(
    UniswapV3Tables,
    "eth_analytics",
    UniV3TickInfo,
    PoolTickInfo,
    "src/sql/tables/"
);

remote_clickhouse_table!(
    UniswapV3Tables,
    "eth_analytics",
    UniV3Slot0,
    PoolSlot0,
    "src/sql/tables/"
);

pub fn spawn_clickhouse_db() -> ClickhouseClient<UniswapV3Tables> {
    let url = std::env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL not found in .env");
    let user = std::env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER not found in .env");
    let pass = std::env::var("CLICKHOUSE_PASS").expect("CLICKHOUSE_PASS not found in .env");

    let config = ClickhouseConfig::new(user, pass, url, true, None);

    info!(target: "uniV3", "started clickhouse db connection");

    config.build()
}

#[derive(Debug, Clone, Serialize, Deserialize, Row, PartialEq)]
pub struct InitialPools {
    #[serde(with = "serde_address")]
    pub pool_address: Address,
    #[serde(with = "serde_address")]
    pub token0_address: Address,
    pub token0_decimals: u8,
    #[serde(with = "serde_address")]
    pub token1_address: Address,
    pub token1_decimals: u8,
    pub creation_block: u64,
}

pub async fn get_initial_pools(
    db: &ClickhouseClient<UniswapV3Tables>,
) -> eyre::Result<(u64, Vec<InitialPools>)> {
    let pools: Vec<InitialPools> = db.query_many(INITIAL_POOLS, &()).await?;

    let min_block = pools.iter().map(|p| p.creation_block).min().unwrap();

    Ok((min_block, pools))
}

pub struct BufferedClickhouse {
    pub db: Arc<ClickhouseClient<UniswapV3Tables>>,
    pub rx: UnboundedReceiver<Vec<PoolData>>,
    pub fut: Option<Pin<Box<dyn Future<Output = eyre::Result<()>> + Send>>>,
    pub queue: Vec<PoolData>,
    pub inserting: Vec<PoolData>,
    pub insert_size: usize,
}
impl BufferedClickhouse {
    pub fn new(
        db: Arc<ClickhouseClient<UniswapV3Tables>>,
        rx: UnboundedReceiver<Vec<PoolData>>,
        insert_size: usize,
    ) -> Self {
        info!(target: "uniV3", "created buffered clickhouse connection");
        Self {
            db,
            rx,
            fut: None,
            queue: Vec::new(),
            inserting: Vec::new(),
            insert_size,
        }
    }

    async fn insert(
        db: Arc<ClickhouseClient<UniswapV3Tables>>,
        vals: Vec<PoolData>,
    ) -> eyre::Result<()> {
        let (tick_info, slot0) = PoolData::combine_many(vals);

        if !tick_info.is_empty() {
            db.insert_many::<UniV3TickInfo>(&tick_info).await?;
        }

        if !slot0.is_empty() {
            db.insert_many::<UniV3Slot0>(&slot0).await?;
        }

        Ok(())
    }
}

impl Future for BufferedClickhouse {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut is_finished = false;

        if let Poll::Ready(inc) = this.rx.poll_recv(cx) {
            if let Some(vals) = inc {
                this.queue.extend(vals);
            } else if this.queue.is_empty() && this.inserting.is_empty() && this.fut.is_none() {
                info!(target: "uniV3", "shutting down clickhouse connection");
                return Poll::Ready(());
            } else {
                is_finished = true;
            }
        }

        let fut = this.fut.take();
        if let Some(mut f) = fut {
            if let Poll::Ready(val) = f.poll_unpin(cx) {
                if let Err(e) = val {
                    let db = this.db.clone();
                    this.fut = Some(Box::pin(Self::insert(db, this.inserting.clone())));
                    error!(target: "uni-v3::db", "error inserting into db, RETRYING - {:?}", e);
                } else {
                    info!(target: "uni-v3::db", "inserted {} values into db", this.inserting.len());
                    this.inserting.clear();
                }
            } else {
                this.fut = Some(f)
            }
        } else if this.queue.len() >= this.insert_size || is_finished {
            this.inserting = this.queue.drain(..).collect::<Vec<_>>();

            let db = this.db.clone();
            this.fut = Some(Box::pin(Self::insert(db, this.inserting.clone())));
        }

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}
