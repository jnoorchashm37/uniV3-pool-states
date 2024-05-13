use std::sync::Arc;

use crate::db::UniV3PoolState;
use crate::db::UniswapV3Tables;
use crate::node::RethDbApiClient;
use crate::state::PoolState;
use alloy_primitives::Address;
use alloy_primitives::U256;
use db_interfaces::clickhouse::client::ClickhouseClient;
use db_interfaces::Database;
use futures::future::join_all;

#[derive(Clone)]
pub struct TickFetcher {
    node: Arc<RethDbApiClient>,
    db: Arc<ClickhouseClient<UniswapV3Tables>>,
    pool: Address,
    min_word: i16,
    max_word: i16,
    tick_spacing: i32,
    pub current_block: u64,
}

impl TickFetcher {
    pub async fn new(
        node: Arc<RethDbApiClient>,
        db: Arc<ClickhouseClient<UniswapV3Tables>>,
        pool: Address,
        initial_block: u64,
    ) -> eyre::Result<Self> {
        let tick_spacing = node.get_tick_spacing(pool, None).await?;

        let min_word = (-887272_i32 >> 8) as i16;
        let max_word = (887272_i32 >> 8) as i16;

        Ok(Self {
            db,
            node,
            pool,
            min_word,
            max_word,
            tick_spacing,
            current_block: initial_block,
        })
    }

    pub async fn execute_block(self) -> Result<(), (u64, eyre::ErrReport)> {
        let state = self
            .get_state_from_ticks()
            .await
            .map_err(|e| (self.current_block, e))?;

        self.insert_values(state)
            .await
            .map_err(|e| (self.current_block, e))?;

        Ok(())
    }

    async fn insert_values(&self, state: Vec<PoolState>) -> eyre::Result<()> {
        Ok(self.db.insert_many::<UniV3PoolState>(&state).await?)
    }

    async fn get_state_from_ticks(&self) -> eyre::Result<Vec<PoolState>> {
        let bitmaps = self.get_bitmaps().await?;
        let ticks = self.get_ticks(bitmaps).await?;

        join_all(ticks.into_iter().map(|tick| async move {
            let tick_return = self
                .node
                .get_state_at_tick(self.pool, tick, self.current_block)
                .await?;

            Ok(PoolState::new_with_block_and_address(
                tick_return,
                self.pool,
                tick,
                self.current_block,
            ))
        }))
        .await
        .into_iter()
        .collect::<eyre::Result<Vec<_>>>()
    }

    async fn get_ticks(&self, bitmaps: Vec<(i16, U256)>) -> eyre::Result<Vec<i32>> {
        let vals = bitmaps
            .into_iter()
            .flat_map(|(idx, map)| {
                if map != U256::ZERO {
                    (0..256)
                        .into_iter()
                        .filter_map(|i| {
                            if (map & (U256::from(1u8) << i)) != U256::ZERO {
                                let tick_index = (idx as i32 * 256 + i) * self.tick_spacing;
                                Some(tick_index)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<_>>();

        Ok(vals)
    }

    async fn get_bitmaps(&self) -> eyre::Result<Vec<(i16, U256)>> {
        join_all(
            (self.min_word..self.max_word)
                .into_iter()
                .map(|i| async move {
                    let bitmap_result = self
                        .node
                        .get_tick_bitmap(self.pool, i, self.current_block)
                        .await?;
                    Ok((i, bitmap_result))
                }),
        )
        .await
        .into_iter()
        .collect::<eyre::Result<Vec<_>>>()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::db::spawn_clickhouse_db;

    use super::*;

    #[tokio::test]
    async fn test_map() {
        dotenv::dotenv().ok();

        let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
        let node = RethDbApiClient::new(&reth_db_path, tokio::runtime::Handle::current())
            .await
            .unwrap();

        let db = spawn_clickhouse_db();

        let fetcher = TickFetcher::new(
            Arc::new(node),
            Arc::new(db),
            Address::from_str("0xCBCdF9626bC03E24f779434178A73a0B4bad62eD").unwrap(),
            19858960,
        )
        .await
        .unwrap();

        let ticks = fetcher.get_state_from_ticks().await.unwrap();

        for t in ticks {
            println!("{:?}", t);
        }
    }
}
