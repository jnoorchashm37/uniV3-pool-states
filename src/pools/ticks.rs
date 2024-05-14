use alloy_primitives::Address;
use alloy_primitives::U256;
use tracing::debug;

use super::PoolDBInner;
use super::PoolState;

#[derive(Clone)]
pub struct TickFetcher {
    pub pool: Address,
    pub min_word: i16,
    pub max_word: i16,
    pub earliest_block: u64,
}

impl TickFetcher {
    pub fn new(pool: Address, earliest_block: u64) -> Self {
        let min_word = (-887272_i32 >> 8) as i16;
        let max_word = (887272_i32 >> 8) as i16;

        Self {
            pool,
            min_word,
            max_word,
            earliest_block,
        }
    }

    pub fn execute_block(
        &self,
        inner: PoolDBInner,
        block_number: u64,
    ) -> eyre::Result<Vec<PoolState>> {
        let state = self.get_state_from_ticks(&inner, block_number)?;

        if state.is_empty() {
            return Ok(Vec::new());
        }

        debug!(target: "uni-v3", "pool: {:?} - got state for block {}", self.pool, block_number);

        Ok(state)
    }

    fn get_state_from_ticks(
        &self,
        inner: &PoolDBInner,
        block_number: u64,
    ) -> eyre::Result<Vec<PoolState>> {
        let bitmaps = self.get_bitmaps(inner)?;
        if bitmaps.is_empty() {
            return Ok(Vec::new());
        }

        let tick_spacing = inner.get_tick_spacing(self.pool)?;
        let ticks = self.get_ticks(bitmaps, tick_spacing)?;

        if ticks.is_empty() {
            return Ok(Vec::new());
        }

        let states = inner.get_state_at_ticks(self.pool, ticks)?;

        Ok(states
            .into_iter()
            .map(|(tick, state)| {
                PoolState::new_with_block_and_address(
                    state,
                    self.pool,
                    tick,
                    block_number,
                    tick_spacing,
                )
            })
            .collect())
    }

    fn get_ticks(&self, bitmaps: Vec<(i16, U256)>, tick_spacing: i32) -> eyre::Result<Vec<i32>> {
        let vals = bitmaps
            .into_iter()
            .flat_map(|(idx, map)| {
                if map != U256::ZERO {
                    (0..256)
                        .into_iter()
                        .filter_map(|i| {
                            if (map & (U256::from(1u8) << i)) != U256::ZERO {
                                let tick_index = (idx as i32 * 256 + i) * tick_spacing;
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

    fn get_bitmaps(&self, inner: &PoolDBInner) -> eyre::Result<Vec<(i16, U256)>> {
        let range = self.min_word..self.max_word;
        if range.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(inner.get_tick_bitmaps(self.pool, range)?)
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
        // dotenv::dotenv().ok();

        // let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
        // let node = RethDbApiClient::new(&reth_db_path, tokio::runtime::Handle::current())
        //     .await
        //     .unwrap();

        // let db = spawn_clickhouse_db();

        // let fetcher = TickFetcher::new(
        //     Arc::new(node),
        //     Arc::new(db),
        //     Address::from_str("0xCBCdF9626bC03E24f779434178A73a0B4bad62eD").unwrap(),
        //     19858960,
        // )
        // .await
        // .unwrap();

        // let ticks = fetcher.get_state_from_ticks().await.unwrap();

        // for t in ticks {
        //     println!("{:?}", t);
        // }
    }
}
