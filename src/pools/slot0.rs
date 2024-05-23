use crate::pools::PoolSlot0;
use crate::utils::u256_to_natural;
use crate::utils::TokenInfo;
use alloy_primitives::Address;
use alloy_primitives::TxHash;
use alloy_primitives::U256;
use malachite::num::arithmetic::traits::Pow;
use malachite::num::conversion::traits::RoundingFrom;
use malachite::rounding_modes::RoundingMode;
use malachite::Natural;
use malachite::Rational;
use tracing::debug;

use super::PoolDBInner;
use super::PoolData;
use super::PoolFetcher;

#[derive(Clone)]
pub struct PoolSlot0Fetcher {
    pub pool_address: Address,
    pub token0: TokenInfo,
    pub token1: TokenInfo,
    pub earliest_block: u64,
}

impl PoolSlot0Fetcher {
    pub fn new(
        pool_address: Address,
        token0: TokenInfo,
        token1: TokenInfo,
        earliest_block: u64,
    ) -> Self {
        Self {
            pool_address,
            token0,
            token1,
            earliest_block,
        }
    }

    fn calculate_price(&self, sqrt_price_x96: U256) -> f64 {
        let sqrt_price = u256_to_natural(sqrt_price_x96);
        let non_adj_price = Rational::from_naturals(sqrt_price.pow(2), Natural::from(2u8).pow(192));

        let decimals_factor = Rational::from_naturals(
            Natural::from(10u8).pow(self.token0.decimals as u64),
            Natural::from(10u8).pow(self.token1.decimals as u64),
        );

        let calculated_price = non_adj_price * decimals_factor;

        f64::rounding_from(calculated_price, RoundingMode::Nearest).0
    }
}

impl PoolFetcher for PoolSlot0Fetcher {
    fn execute_block(
        &self,
        inner: &mut PoolDBInner,
        block_number: u64,
        tx_hash: TxHash,
        tx_index: u64,
    ) -> eyre::Result<Vec<PoolData>> {
        let slot0 = inner.get_slot0(self.pool_address)?;

        let calculated_price = self.calculate_price(slot0.sqrtPriceX96);

        let data = PoolSlot0::new(
            slot0,
            self.pool_address,
            tx_hash,
            tx_index,
            block_number,
            &self.token0,
            &self.token1,
            calculated_price,
        );

        debug!(target: "uni-v3::data::slot0", "pool: {:?} - got slot0 for block {} and tx hash {:?}", self.pool_address, block_number, tx_hash);

        Ok(vec![data.into()])
    }

    fn earliest_block(&self) -> u64 {
        self.earliest_block
    }

    fn pool_address(&self) -> Address {
        self.pool_address
    }
}

/*


SELECT
    exchange,
    'ETH-USD' AS eth,
    any(timestamp) AS time,
    any((ask_price+bid_price)/2) AS eth_price
FROM cex.normalized_quotes
WHERE symbol LIKE 'ETH%' AND (symbol LIKE '%USDC' OR symbol LIKE '%USDT') AND timestamp >= (1702746431) * 1000000 AND timestamp < (1702746431 + 12) * 1000000
GROUP BY exchange


1288329390478420389134906353335981^2 / 2^192


264525828.74 * 10


1/.00026

ETH/USDC

*/

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use crate::node::EthNodeApi;

    use super::*;

    #[tokio::test]
    async fn test_slot0() {
        dotenv::dotenv().ok();

        let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
        let node = EthNodeApi::new(&reth_db_path, tokio::runtime::Handle::current()).unwrap();

        let test_block_number = 19933988;
        let pool_address = Address::from_str("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640").unwrap();

        let token0 = Address::from_str("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap();
        let token0_decimals = 6;

        let token1 = Address::from_str("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2").unwrap();
        let token1_decimals = 18;

        let mut pool_inner = PoolDBInner::new(Arc::new(node), test_block_number)
            .await
            .unwrap();

        let test_ticker = PoolSlot0Fetcher::new(
            pool_address,
            TokenInfo::new(token0, token0_decimals),
            TokenInfo::new(token1, token1_decimals),
            12376729,
        );

        let tx_hash =
            TxHash::from_str("0x7f96b7c6186be132d7032ee9e42221250bf9720b997b0905447a8a73513c51d8")
                .unwrap();
        let calculated = test_ticker
            .execute_block(&mut pool_inner, test_block_number, tx_hash, 88)
            .unwrap();
        let expected = PoolData::Slot0(PoolSlot0 {
            block_number: test_block_number,
            pool_address,
            tx_hash,
            tx_index: 88,
            tick: 193887,
            token0,
            token0_decimals,
            token1,
            token1_decimals,
            sqrt_price_x96: U256::from(1284929393637281108785461745518480u128),
            calculated_price: 0.00006,
            observation_index: 125,
            observation_cardinality: 723,
            observation_cardinality_next: 723,
            fee_protocol: 0,
            unlocked: true,
        });

        assert!(calculated.contains(&expected));
    }
}
