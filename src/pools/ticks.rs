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
        Self {
            pool,
            min_word: (-887272_i32 >> 8) as i16,
            max_word: (887272_i32 >> 8) as i16,
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
    use std::{str::FromStr, sync::Arc};

    use crate::node::RethDbApiClient;

    use super::*;

    #[tokio::test]
    async fn test_map() {
        dotenv::dotenv().ok();

        let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
        let node = RethDbApiClient::new(&reth_db_path, tokio::runtime::Handle::current())
            .await
            .unwrap();

        let pool_inner = PoolDBInner::new(Arc::new(node), 19000000).await.unwrap();

        let test_ticker = TickFetcher::new(
            Address::from_str("0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8").unwrap(),
            12369854,
        );

        let calculated = test_ticker.execute_block(pool_inner, 12370244).unwrap();
        let expected = vec![
            PoolState {
                block_number: 12370244,
                pool_address: "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8".to_string(),
                tick: -84120,
                tick_spacing: 60,
                liquidity_gross: 121672637676928310822,
                liquidity_net: 121672637676928310822,
                fee_growth_outside_0_x128: U256::from(0u64),
                fee_growth_outside_1_x128: U256::from(0u64),
                tick_cumulative_outside: 0,
                seconds_per_liquidity_outside_x128: U256::from(0u64),
                seconds_outside: 1620159368,
                initialized: true,
            },
            PoolState {
                block_number: 12370244,
                pool_address: "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8".to_string(),
                tick: -82920,
                tick_spacing: 60,
                liquidity_gross: 7079623107842667994,
                liquidity_net: 7079623107842667994,
                fee_growth_outside_0_x128: U256::from(0u64),
                fee_growth_outside_1_x128: U256::from(127510492160377860903322733723813u128),
                tick_cumulative_outside: -167630257,
                seconds_per_liquidity_outside_x128: U256::from(8186109579676388573702u128),
                seconds_outside: 1620161431,
                initialized: true,
            },
            PoolState {
                block_number: 12370244,
                pool_address: "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8".to_string(),
                tick: -81900,
                tick_spacing: 60,
                liquidity_gross: 100961002448877659420,
                liquidity_net: 100961002448877659420,
                fee_growth_outside_0_x128: U256::from(0u64),
                fee_growth_outside_1_x128: U256::from(127510492160377860903322733723813u128),
                tick_cumulative_outside: -151464691,
                seconds_per_liquidity_outside_x128: U256::from(7881949835537984141608u128),
                seconds_outside: 1620161232,
                initialized: true,
            },
            PoolState {
                block_number: 12370244,
                pool_address: "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8".to_string(),
                tick: -80040,
                tick_spacing: 60,
                liquidity_gross: 100961002448877659420,
                liquidity_net: -100961002448877659420,
                fee_growth_outside_0_x128: U256::from(0u64),
                fee_growth_outside_1_x128: U256::from(0u64),
                tick_cumulative_outside: 0,
                seconds_per_liquidity_outside_x128: U256::from(0u64),
                seconds_outside: 0,
                initialized: true,
            },
            PoolState {
                block_number: 12370244,
                pool_address: "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8".to_string(),
                tick: -79560,
                tick_spacing: 60,
                liquidity_gross: 7079623107842667994,
                liquidity_net: -7079623107842667994,
                fee_growth_outside_0_x128: U256::from(0u64),
                fee_growth_outside_1_x128: U256::from(0u64),
                tick_cumulative_outside: 0,
                seconds_per_liquidity_outside_x128: U256::from(0u64),
                seconds_outside: 0,
                initialized: true,
            },
            PoolState {
                block_number: 12370244,
                pool_address: "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8".to_string(),
                tick: -78240,
                tick_spacing: 60,
                liquidity_gross: 121672637676928310822,
                liquidity_net: -121672637676928310822,
                fee_growth_outside_0_x128: U256::from(0u64),
                fee_growth_outside_1_x128: U256::from(0u64),
                tick_cumulative_outside: 0,
                seconds_per_liquidity_outside_x128: U256::from(0u64),
                seconds_outside: 0,
                initialized: true,
            },
        ];

        assert_eq!(calculated, expected);
    }
}
