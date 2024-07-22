use super::PoolFetcher;
use crate::node::FilteredTraceCall;

use crate::pools::types::PoolData;

use crate::pools::types::PoolTrade;
use crate::pools::UniswapV3;

use crate::utils::TokenInfo;
use alloy_primitives::Address;

use alloy_sol_types::SolCall;

use tracing::debug;

#[derive(Clone)]
pub struct PoolTradeFetcher {
    pub pool_address: Address,
    pub token0: TokenInfo,
    pub token1: TokenInfo,
    pub earliest_block: u64,
}

impl PoolTradeFetcher {
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
}

impl PoolFetcher for PoolTradeFetcher {
    fn is_re_executed(&self) -> bool {
        false
    }
    fn is_decoded(&self) -> bool {
        true
    }

    fn decode_block(
        &self,
        block_number: u64,
        tx_calls: &[FilteredTraceCall],
    ) -> eyre::Result<Vec<PoolData>> {
        let mut data = Vec::new();

        tx_calls
            .iter()
            .map(|call| {
                if call.func_sig == UniswapV3::swapCall::SELECTOR {
                    let call_input = UniswapV3::swapCall::abi_decode(&call.input, false)?;
                    let call_output = UniswapV3::swapCall::abi_decode_returns(&call.output, false)?;
                    data.push(PoolData::Trade(PoolTrade::new(
                        call_input,
                        call_output,
                        self.pool_address(),
                        call.tx_hash,
                        block_number,
                        &self.token0,
                        &self.token1,
                    )))
                }

                Ok::<_, eyre::ErrReport>(())
            })
            .collect::<eyre::Result<Vec<_>>>()?;

        debug!(target: "uniV3::data::trades", "pool: {:?} - got {} trades for block {}", self.pool_address,data.len(), block_number);

        Ok(data)
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
    use std::str::FromStr;

    use alloy_primitives::I256;

    use crate::node::{filter_traces_by_address_to_call_input, EthNodeApi};

    use super::*;

    #[tokio::test]
    async fn test_slot0() {
        dotenv::dotenv().ok();

        let reth_db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
        let node = EthNodeApi::new(&reth_db_path, tokio::runtime::Handle::current()).unwrap();

        let test_block_number = 20364223;
        let pool_address = Address::from_str("0x5777d92f208679db4b9778590fa3cab3ac9e2168").unwrap();

        let token0 = Address::from_str("0x6B175474E89094C44Da98b954EedeAC495271d0F").unwrap();
        let token0_decimals = 18;
        let token0_amount: i128 = -195184845081919051330;

        let token1 = Address::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap();
        let token1_decimals = 6;
        let token1_amount = 195208636;

        let calculated_price = f64::rounding_from(
            Rational::from_naturals(
                Natural::from(195184845081919051330u128),
                Natural::from(195208636u128),
            ) / Rational::from_naturals(Natural::from(18u8), Natural::from(6u8)),
            RoundingMode::Nearest,
        )
        .0;

        let test_ticker = PoolTradeFetcher::new(
            pool_address,
            TokenInfo::new(token0, token0_decimals),
            TokenInfo::new(token1, token1_decimals),
            12376729,
        );

        let pool_txs = node
            .get_filtered_transaction_traces(test_block_number, |tx| {
                filter_traces_by_address_to_call_input(tx, &[pool_address])
            })
            .await
            .unwrap()
            .into_iter()
            .map(|(_, t)| t)
            .collect::<Vec<_>>();

        let calculated = test_ticker
            .decode_block(test_block_number, &pool_txs)
            .unwrap();

        let expected = PoolData::Trade(PoolTrade {
            block_number: test_block_number,
            pool_address,
            tx_hash: TxHash::from_str(
                "0x1d6da6139d17a2ed774997d2c1928409dd934032e9e39fea2f01541b7774e852",
            )
            .unwrap(),
            token_in: token1,
            token_in_decimals: token1_decimals,
            token_in_amount: I256::try_from(token1_amount).unwrap(),
            token_out: token0,
            token_out_decimals: token0_decimals,
            token_out_amount: I256::try_from(token0_amount).unwrap(),
            calculated_price,
        });

        for t in &calculated {
            println!("{:?}\n", t);
        }

        assert!(calculated.contains(&expected));
    }
}
