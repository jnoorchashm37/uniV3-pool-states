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
use tracing::info;

#[derive(Clone)]
pub struct TickFetcher {
    node: Arc<RethDbApiClient>,
    db: Arc<ClickhouseClient<UniswapV3Tables>>,
    pub pool: Address,
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

        if state.is_empty() {
            return Ok(());
        }

        self.insert_values(state)
            .await
            .map_err(|e| (self.current_block, e))?;

        info!(target: "uni-v3", "pool: {:?} - completed block {}", self.pool, self.current_block);

        Ok(())
    }

    async fn insert_values(&self, state: Vec<PoolState>) -> eyre::Result<()> {
        Ok(self.db.insert_many::<UniV3PoolState>(&state).await?)
    }

    async fn get_state_from_ticks(&self) -> eyre::Result<Vec<PoolState>> {
        let bitmaps = self.get_bitmaps().await?;
        if bitmaps.is_empty() {
            return Ok(Vec::new());
        }

        let ticks = self.get_ticks(bitmaps)?;

        if ticks.is_empty() {
            return Ok(Vec::new());
        }

        let states = self
            .node
            .get_state_at_ticks(self.pool, ticks, self.current_block)
            .await?;

        Ok(states
            .into_iter()
            .map(|(tick, state)| {
                PoolState::new_with_block_and_address(state, self.pool, tick, self.current_block)
            })
            .collect())
    }

    fn get_ticks(&self, bitmaps: Vec<(i16, U256)>) -> eyre::Result<Vec<i32>> {
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
        let range = self.min_word..self.max_word;
        if range.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(self
                .node
                .get_tick_bitmaps(self.pool, range, self.current_block)
                .await?)
        }
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
