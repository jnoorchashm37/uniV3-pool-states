use alloy_primitives::{Address, U256};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::contracts::UniswapV3;

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct PoolState {
    pub block_number: u64,
    pub pool_address: String,
    pub tick: i32,
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
    #[serde(with = "u256")]
    pub fee_growth_outside_0_x128: U256,
    #[serde(with = "u256")]
    pub fee_growth_outside_1_x128: U256,
    pub tick_cumulative_outside: i64,
    #[serde(with = "u256")]
    pub seconds_per_liquidity_outside_x128: U256,
    pub seconds_outside: u32,
    pub initialized: bool,
}

impl PoolState {
    pub fn new_with_block_and_address(
        tick_return: UniswapV3::ticksReturn,
        address: Address,
        tick: i32,
        block_number: u64,
    ) -> Self {
        Self {
            block_number,
            pool_address: format!("{:?}", address),
            tick,
            liquidity_gross: tick_return.liquidityGross,
            liquidity_net: tick_return.liquidityNet,
            fee_growth_outside_0_x128: tick_return.feeGrowthOutside0X128,
            fee_growth_outside_1_x128: tick_return.feeGrowthOutside1X128,
            tick_cumulative_outside: tick_return.tickCumulativeOutside,
            seconds_per_liquidity_outside_x128: tick_return.secondsPerLiquidityOutsideX128,
            seconds_outside: tick_return.secondsOutside,
            initialized: tick_return.initialized,
        }
    }
}

mod u256 {
    use alloy_primitives::U256;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    pub fn serialize<S: Serializer>(u: &U256, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes: [u8; 32] = u.to_le_bytes();
        bytes.serialize(serializer)
    }

    #[allow(dead_code)]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(U256::from_le_bytes(u))
    }
}
