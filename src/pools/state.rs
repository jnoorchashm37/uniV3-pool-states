use alloy_primitives::{Address, TxHash, U256};

use clickhouse::Row;

use serde::{Deserialize, Serialize};

use crate::pools::UniswapV3;
use crate::utils::*;

#[derive(Debug, Clone, Serialize, Deserialize, Row, PartialEq)]
pub struct PoolTickInfo {
    pub block_number: u64,
    #[serde(with = "serde_address")]
    pub pool_address: Address,
    #[serde(with = "serde_tx_hash")]
    pub tx_hash: TxHash,
    pub tx_index: u64,
    pub tick: i32,
    pub tick_spacing: i32,
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
    #[serde(with = "serde_u256")]
    pub fee_growth_outside_0_x128: U256,
    #[serde(with = "serde_u256")]
    pub fee_growth_outside_1_x128: U256,
    pub tick_cumulative_outside: i64,
    #[serde(with = "serde_u256")]
    pub seconds_per_liquidity_outside_x128: U256,
    pub seconds_outside: u32,
    pub initialized: bool,
}

impl PoolTickInfo {
    pub fn new_with_block_and_address(
        tick_return: UniswapV3::ticksReturn,
        pool_address: Address,
        tx_hash: TxHash,
        tx_index: u64,
        tick: i32,
        block_number: u64,
        tick_spacing: i32,
    ) -> Self {
        Self {
            block_number,
            pool_address,
            tx_hash,
            tx_index,
            tick,
            tick_spacing,
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

#[derive(Debug, Clone, Serialize, Deserialize, Row, PartialEq)]
pub struct PoolSlot0 {
    pub block_number: u64,
    #[serde(with = "serde_address")]
    pub pool_address: Address,
    #[serde(with = "serde_address")]
    pub token0: Address,
    pub token0_decimals: u8,
    #[serde(with = "serde_address")]
    pub token1: Address,
    pub token1_decimals: u8,
    #[serde(with = "serde_tx_hash")]
    pub tx_hash: TxHash,
    pub tx_index: u64,
    pub tick: i32,
    #[serde(with = "serde_u256")]
    pub sqrt_price_x96: U256,
    pub calculated_price: f64,
    pub observation_index: u16,
    pub observation_cardinality: u16,
    pub observation_cardinality_next: u16,
    pub fee_protocol: u8,
    pub unlocked: bool,
}

impl PoolSlot0 {
    pub fn new(
        slot0_return: UniswapV3::slot0Return,
        pool_address: Address,
        tx_hash: TxHash,
        tx_index: u64,
        block_number: u64,
        token0: &TokenInfo,
        token1: &TokenInfo,
        calculated_price: f64,
    ) -> Self {
        Self {
            block_number,
            pool_address,
            tx_hash,
            tx_index,
            tick: slot0_return.tick,
            token0: token0.address,
            token0_decimals: token0.decimals,
            token1: token1.address,
            token1_decimals: token1.decimals,
            sqrt_price_x96: slot0_return.sqrtPriceX96,
            calculated_price,
            observation_index: slot0_return.observationIndex,
            observation_cardinality: slot0_return.observationCardinality,
            observation_cardinality_next: slot0_return.observationCardinalityNext,
            fee_protocol: slot0_return.feeProtocol,
            unlocked: slot0_return.unlocked,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PoolData {
    TickInfo(PoolTickInfo),
    Slot0(PoolSlot0),
}

impl PoolData {
    pub fn combine_many(values: Vec<Self>) -> (Vec<PoolTickInfo>, Vec<PoolSlot0>) {
        let mut tick_info = Vec::new();
        let mut slot0 = Vec::new();

        values.into_iter().for_each(|v| match v {
            PoolData::TickInfo(val) => tick_info.push(val),
            PoolData::Slot0(val) => slot0.push(val),
        });

        (tick_info, slot0)
    }
}

macro_rules! to_pool_data {
    ($($dt:ident),*) => {

        $(
            paste::paste! {

                impl From<[<Pool $dt>]> for PoolData {
                    fn from(value: [<Pool $dt>]) -> PoolData {
                        PoolData::$dt(value)
                    }
                }
            }
        )*
    };
}

to_pool_data!(Slot0, TickInfo);
