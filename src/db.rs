use std::{
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
};

use db_interfaces::{
    clickhouse::{client::ClickhouseClient, config::ClickhouseConfig},
    clickhouse_dbms, remote_clickhouse_table, Database,
};
use futures::{Future, FutureExt};
use itertools::Itertools;
use reth_primitives::Address;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info};

use crate::pools::{PoolState, PoolTickFetcher};

clickhouse_dbms!(UniswapV3Tables, [UniV3PoolState]);

remote_clickhouse_table!(
    UniswapV3Tables,
    "eth_analytics",
    UniV3PoolState,
    PoolState,
    "src/pools/sql/"
);

pub fn spawn_clickhouse_db() -> ClickhouseClient<UniswapV3Tables> {
    let url = std::env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL not found in .env");
    let user = std::env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER not found in .env");
    let pass = std::env::var("CLICKHOUSE_PASS").expect("CLICKHOUSE_PASS not found in .env");

    let config = ClickhouseConfig::new(user, pass, url, false, None);

    config.build()
}

pub const INITIAL_POOLS_QUERY: &str = "
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

pub async fn get_initial_pools(
    db: &ClickhouseClient<UniswapV3Tables>,
) -> eyre::Result<(u64, Vec<PoolTickFetcher>)> {
    let initial_pools: Vec<(String, u64)> = db.query_many(INITIAL_POOLS_QUERY, &()).await?;

    let pools = initial_pools
        .into_iter()
        .map(|(addr, blk)| PoolTickFetcher::new(Address::from_str(&addr).unwrap(), blk))
        .collect::<Vec<_>>();

    let min_block = pools.iter().map(|p| p.earliest_block).min().unwrap();

    Ok((min_block, pools))
}

pub struct BufferedClickhouse {
    pub db: ClickhouseClient<UniswapV3Tables>,
    pub rx: UnboundedReceiver<Vec<PoolState>>,
    pub fut: Option<Pin<Box<dyn Future<Output = eyre::Result<()>> + Send>>>,
    pub queue: Vec<PoolState>,
    pub inserting: Vec<PoolState>,
    pub insert_size: usize,
}
impl BufferedClickhouse {
    pub fn new(
        db: ClickhouseClient<UniswapV3Tables>,
        rx: UnboundedReceiver<Vec<PoolState>>,
        insert_size: usize,
    ) -> Self {
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
        db: ClickhouseClient<UniswapV3Tables>,
        vals: Vec<PoolState>,
    ) -> eyre::Result<()> {
        Ok(db.insert_many::<UniV3PoolState>(&vals).await?)
    }
}

impl Future for BufferedClickhouse {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        println!("POLLING");

        if let Poll::Ready(inc) = this.rx.poll_recv(cx) {
            if let Some(vals) = inc {
                this.queue.extend(vals);
            } else {
                return Poll::Ready(());
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
        }

        if this.fut.is_none() {
            if this.queue.len() >= this.insert_size {
                this.inserting = this.queue.drain(..this.insert_size).collect_vec();

                let db = this.db.clone();
                this.fut = Some(Box::pin(Self::insert(db, this.inserting.clone())));
            }
        }

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}
