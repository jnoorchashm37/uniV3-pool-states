use alloy_primitives::Address;
use alloy_primitives::TxHash;
use alloy_primitives::U256;
use tracing::debug;

use super::PoolDBInner;
use super::PoolFetcher;
use crate::pools::types::PoolData;
use crate::pools::types::PoolTickInfo;

#[derive(Clone)]
pub struct PoolTickFetcher {
    pub pool_address: Address,
    pub min_word: i16,
    pub max_word: i16,
    pub earliest_block: u64,
}

impl PoolTickFetcher {
    pub fn new(pool_address: Address, earliest_block: u64) -> Self {
        Self {
            pool_address,
            min_word: (-887272_i32 >> 8) as i16,
            max_word: (887272_i32 >> 8) as i16,
            earliest_block,
        }
    }

    fn get_state_from_ticks(
        &self,
        inner: &mut PoolDBInner,
        block_number: u64,
        tx_hash: TxHash,
        tx_index: u64,
    ) -> eyre::Result<Vec<PoolTickInfo>> {
        let bitmaps = inner.get_tick_bitmaps(self.pool_address, self.min_word..self.max_word)?;
        if bitmaps.is_empty() {
            return Ok(Vec::new());
        }

        let tick_spacing = inner.get_tick_spacing(self.pool_address)?;
        let ticks = self.get_ticks(bitmaps, tick_spacing)?;

        if ticks.is_empty() {
            return Ok(Vec::new());
        }

        let states = inner.get_state_at_ticks(self.pool_address, ticks)?;

        Ok(states
            .into_iter()
            .map(|(tick, state)| {
                PoolTickInfo::new_with_block_and_address(
                    state,
                    self.pool_address,
                    tx_hash,
                    tx_index,
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
                    Vec::new()
                }
            })
            .collect::<Vec<_>>();

        Ok(vals)
    }
}

impl PoolFetcher for PoolTickFetcher {
    fn is_re_executed(&self) -> bool {
        true
    }
    fn is_decoded(&self) -> bool {
        false
    }

    fn re_execute_block(
        &self,
        inner: &mut PoolDBInner,
        block_number: u64,
        tx_hash: TxHash,
        tx_index: u64,
    ) -> eyre::Result<Vec<PoolData>> {
        let state = self.get_state_from_ticks(inner, block_number, tx_hash, tx_index)?;

        if state.is_empty() {
            return Ok(Vec::new());
        }

        debug!(target: "uniV3::data::tick-info", "pool: {:?} - got state for block {} and tx hash {:?}", self.pool_address, block_number, tx_hash);

        Ok(state.into_iter().map(Into::into).collect())
    }

    fn earliest_block(&self) -> u64 {
        self.earliest_block
    }

    fn pool_address(&self) -> Address {
        self.pool_address
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use crate::node::EthNodeApi;

    use super::*;

    #[tokio::test]
    async fn test_map() {
        dotenv::dotenv().ok();

        let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
        let node = EthNodeApi::new(&reth_db_path, tokio::runtime::Handle::current()).unwrap();

        let mut pool_inner = PoolDBInner::new(Arc::new(node), 12369879).await.unwrap();

        let test_ticker = PoolTickFetcher::new(
            Address::from_str("0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8").unwrap(),
            12369854,
        );

        let tx_hash =
            TxHash::from_str("0x2bdb4298b35adf058a38dfbe85470f67da1cb76e169496f9fa04fd19fb153274")
                .unwrap();
        let calculated = test_ticker
            .re_execute_block(&mut pool_inner, 12369879, tx_hash, 253)
            .unwrap();
        let expected = vec![
            PoolData::TickInfo(PoolTickInfo {
                block_number: 12369879,
                pool_address: Address::from_str("0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8")
                    .unwrap(),
                tx_hash,
                tx_index: 253,
                tick: -84120,
                tick_spacing: 60,
                liquidity_gross: 80059851033970806503,
                liquidity_net: 80059851033970806503,
                fee_growth_outside_0_x128: U256::from(0u64),
                fee_growth_outside_1_x128: U256::from(0u64),
                tick_cumulative_outside: 0,
                seconds_per_liquidity_outside_x128: U256::from(0u64),
                seconds_outside: 1620159368,
                initialized: true,
            }),
            PoolData::TickInfo(PoolTickInfo {
                block_number: 12369879,
                pool_address: Address::from_str("0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8")
                    .unwrap(),
                tx_hash,
                tx_index: 253,
                tick: -78240,
                tick_spacing: 60,
                liquidity_gross: 80059851033970806503,
                liquidity_net: -80059851033970806503,
                fee_growth_outside_0_x128: U256::from(0u64),
                fee_growth_outside_1_x128: U256::from(0u64),
                tick_cumulative_outside: 0,
                seconds_per_liquidity_outside_x128: U256::from(0u64),
                seconds_outside: 0,
                initialized: true,
            }),
        ];

        assert_eq!(calculated, expected);
    }
}
