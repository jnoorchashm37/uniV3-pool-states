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
                    error!(target: "uniV3::db", "error inserting into db, RETRYING - {:?}", e);
                } else {
                    info!(target: "uniV3::db", "inserted {} values into db", this.inserting.len());
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use time::OffsetDateTime;

    use super::*;

    const DEX_QUERY: &str = r#"
        WITH 
            19377595 AS start_block,
            19377650 AS end_block,
            last_state AS (
                SELECT
                    block_number,
                    pool_address,
                    argMax(calculated_price, tx_index) AS price
                FROM eth_analytics.uni_v3_slot0
                WHERE pool_address = ? AND block_number >= start_block AND block_number < end_block
                GROUP BY block_number, pool_address
                ORDER BY block_number DESC 
            )
        SELECT
            CAST(b.block_number, 'UInt64') AS block_number,
            toDateTime(b.block_timestamp) AS block_timestamp,
            toString(s.pool_address) AS pool_address,
            s.price AS price
        FROM ethereum.blocks b 
        INNER JOIN last_state s ON b.block_number = s.block_number
        WHERE b.block_number >= start_block AND b.block_number < end_block
        "#;

    #[derive(Debug, Clone, Serialize, Deserialize, Row, PartialEq)]
    pub struct Dex {
        block_number: u64,
        #[serde(with = "clickhouse::serde::time::datetime")]
        block_timestamp: time::OffsetDateTime,
        pool_address: String,
        price: f64,
    }

    pub fn spawn_clickhouse_db_here() -> ClickhouseClient<KarthikTables> {
        let url = std::env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL not found in .env");
        let user = std::env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER not found in .env");
        let pass = std::env::var("CLICKHOUSE_PASS").expect("CLICKHOUSE_PASS not found in .env");

        let config = ClickhouseConfig::new(user, pass, url, true, None);

        info!(target: "uniV3", "started clickhouse db connection");

        config.build()
    }

    clickhouse_dbms!(KarthikTables, [KarthikDex]);
    remote_clickhouse_table!(KarthikTables, "default", KarthikDex, Dex, "src/sql/tables/");

    #[tokio::test]
    async fn karthik() {
        dotenv::dotenv().ok();

        let db = spawn_clickhouse_db_here();
        let mut eth: Vec<Dex> = db
            .query_many(DEX_QUERY, &("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"))
            .await
            .unwrap();
        println!("ETH: {}", eth.len());
        eth.sort_by(|a, b| a.block_number.cmp(&b.block_number));

        let mut map = HashMap::new();
        let mut last = eth.first().cloned().unwrap();

        eth.into_iter().for_each(|eth| {
            if eth.block_number <= 19377600 {
                map.insert(19377600, eth.clone());
                last = eth;
            } else if eth.block_number == std::cmp::max(last.block_number, 19377600) + 1 {
                println!("INSERTED -- {}", eth.block_number);
                last = eth.clone();
                map.insert(eth.block_number, eth);
            } else {
                let mut last_num = std::cmp::max(last.block_number, 19377600) + 1;
                let mut this_last_time = std::cmp::max(
                    last.block_timestamp,
                    OffsetDateTime::parse(
                        "2024-03-06 17:23:59 +00:00:00",
                        time::macros::format_description!(
                            "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                             sign:mandatory]:[offset_minute]:[offset_second]"
                        ),
                    )
                    .unwrap(),
                )
                .checked_add(time::Duration::seconds(12))
                .unwrap();
                println!("ETH: {}", eth.block_number);
                println!("LAST: {}", last_num);
                //panic!();

                while eth.block_number > last_num {
                    //println!("{}", last_num);
                    let mut this_last = last.clone();
                    this_last.block_number = last_num;
                    this_last.block_timestamp = this_last_time;
                    println!("INSERTED -- {last_num}");
                    map.insert(last_num, this_last);
                    last_num += 1;
                    this_last_time = this_last_time
                        .checked_add(time::Duration::seconds(12))
                        .unwrap();
                }
                println!("INSERTED -- {}", eth.block_number);
                map.insert(eth.block_number, eth.clone());

                println!("SET LAST -- {}", eth.block_number);
                last = eth.clone();
            }
        });

        let mut last_num = std::cmp::max(last.block_number, 19377600);
        while 19377650 != last_num {
            println!("{}", last_num);
            let mut this_last = last.clone();
            this_last.block_number = last_num;
            map.insert(last_num, this_last);
            last_num += 1;
        }

        assert_eq!(map.len(), 50);

        db.insert_many::<KarthikDex>(&map.values().cloned().collect::<Vec<_>>())
            .await
            .unwrap();

        let t = 1;

        //(18800000..18800050).map(|val|)
    }
}

/*

OR pool_address = '0xcbcdf9626bc03e24f779434178a73a0b4bad62ed')
0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640

*/
