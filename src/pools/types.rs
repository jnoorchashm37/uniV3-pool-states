use alloy_primitives::{Address, TxHash, I256, U256};

use clickhouse::Row;
use malachite::rounding_modes::RoundingMode;
use malachite::strings::ToDebugString;

use crate::pools::UniswapV3;
use crate::utils::*;
use malachite::num::conversion::traits::RoundingFrom;
use malachite::{Natural, Rational};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize, Row, PartialEq)]
pub struct PoolTrade {
    pub block_number: u64,
    #[serde(with = "serde_tx_hash")]
    pub tx_hash: TxHash,
    #[serde(with = "serde_address")]
    pub pool_address: Address,
    #[serde(with = "serde_address")]
    pub token_in: Address,
    pub token_in_decimals: u8,
    #[serde(with = "serde_i256")]
    pub token_in_amount: I256,
    #[serde(with = "serde_address")]
    pub token_out: Address,
    pub token_out_decimals: u8,
    #[serde(with = "serde_i256")]
    pub token_out_amount: I256,
    pub calculated_price: f64,
}

impl PoolTrade {
    pub fn new(
        swap_call: UniswapV3::swapCall,
        swap_return: UniswapV3::swapReturn,
        pool_address: Address,
        tx_hash: TxHash,
        block_number: u64,
        token0: &TokenInfo,
        token1: &TokenInfo,
    ) -> Self {
        let (
            (token_in, token_in_decimals, token_in_amount),
            (token_out, token_out_decimals, token_out_amount),
        ) = if swap_call.zeroForOne {
            (
                (token1.address, token1.decimals, swap_return.amount1),
                (token0.address, token0.decimals, swap_return.amount0),
            )
        } else {
            (
                (token0.address, token0.decimals, swap_return.amount0),
                (token1.address, token1.decimals, swap_return.amount1),
            )
        };

        let token_in_natural = u256_to_natural(token_in_amount.abs().try_into().unwrap());
        // println!("IN: {:?}", token_in_natural.to_debug_string());
        let token_out_natural = u256_to_natural(token_out_amount.abs().try_into().unwrap());
        //  println!("OUT: {:?}", token_out_natural.to_debug_string());

        let calculated_price = f64::rounding_from(
            Rational::from_naturals(token_in_natural, token_out_natural)
                / Rational::from_naturals(
                    Natural::from(token_in_decimals),
                    Natural::from(token_out_decimals),
                ),
            RoundingMode::Nearest,
        )
        .0;

        Self {
            block_number,
            pool_address,
            tx_hash,
            token_in,
            token_in_decimals,
            token_in_amount,
            token_out,
            token_out_decimals,
            token_out_amount,
            calculated_price,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PoolData {
    TickInfo(PoolTickInfo),
    Slot0(PoolSlot0),
    Trade(PoolTrade),
}

impl PoolData {
    pub fn combine_many(values: Vec<Self>) -> (Vec<PoolTickInfo>, Vec<PoolSlot0>, Vec<PoolTrade>) {
        let mut tick_info = Vec::new();
        let mut slot0 = Vec::new();
        let mut trades = Vec::new();

        values.into_iter().for_each(|v| match v {
            PoolData::TickInfo(val) => tick_info.push(val),
            PoolData::Slot0(val) => slot0.push(val),
            PoolData::Trade(trade) => trades.push(trade),
        });

        (tick_info, slot0, trades)
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

to_pool_data!(Slot0, TickInfo, Trade);
