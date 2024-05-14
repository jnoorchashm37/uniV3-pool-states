use crate::{execute_on_threadpool, node::RethDbApiClient};
use alloy_primitives::Address;
use alloy_sol_types::SolCall;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reth_primitives::U256;
use reth_provider::StateProvider;
use reth_revm::{
    database::StateProviderDatabase,
    db::CacheDB,
    primitives::{BlockEnv, EnvWithHandlerCfg, TransactTo, TxEnv},
};
use reth_rpc::eth::EthTransactions;
use std::{ops::Range, sync::Arc};
use tokio::sync::mpsc::UnboundedSender;
use tracing::info;

use super::{PoolState, PoolTickFetcher, UniswapV3};

pub struct PoolCaller<'a> {
    pub node: Arc<RethDbApiClient>,
    pub db_tx: UnboundedSender<Vec<PoolState>>,
    pub pools: Vec<&'a PoolTickFetcher>,
    pub block_number: u64,
}

impl<'a> PoolCaller<'a> {
    pub fn new(
        node: Arc<RethDbApiClient>,
        db_tx: UnboundedSender<Vec<PoolState>>,
        pools: &'a [PoolTickFetcher],
        block_number: u64,
    ) -> Self {
        let pools = pools
            .iter()
            .filter(|pool| pool.earliest_block <= block_number)
            .collect::<Vec<_>>();
        Self {
            node,
            db_tx,
            pools,
            block_number,
        }
    }

    pub async fn execute_block(self) -> Result<usize, (u64, eyre::ErrReport)> {
        self.run_block()
            .await
            .map_err(|e| (self.block_number, e.into()))?;

        Ok(self.pools.len())
    }

    async fn run_block(&self) -> eyre::Result<()> {
        let pool_inner = PoolDBInner::new(self.node.clone(), self.block_number).await?;

        let state = execute_on_threadpool(|| self.run_cycle(&pool_inner, &self.pools))?;
        info!(target: "uni-v3::fetcher", "completed block {} for {} pools with {} total ticks", self.block_number,self.pools.len(), state.len());

        self.db_tx.send(state)?;

        Ok(())
    }

    fn run_cycle(
        &self,
        inner: &PoolDBInner,
        pools: &[&PoolTickFetcher],
    ) -> eyre::Result<Vec<PoolState>> {
        let state = pools
            .par_iter()
            .map(|pool| {
                let block_number = self.block_number;
                pool.execute_block(inner, block_number)
            })
            .collect::<eyre::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(state)
    }
}

#[derive(Clone)]
pub struct PoolDBInner {
    pub node: Arc<RethDbApiClient>,
    pub state_db: Arc<StateProviderDatabase<Box<dyn StateProvider>>>,
    pub env: EnvWithHandlerCfg,
    pub block_env: BlockEnv,
}

impl PoolDBInner {
    pub async fn new(node: Arc<RethDbApiClient>, block_number: u64) -> eyre::Result<Self> {
        let state_db = node.state_provider_db(block_number)?;
        let (cfg_env, block_env, _) = node.get_evm_env_at(block_number).await?;

        Ok(Self {
            node,
            state_db: Arc::new(state_db),
            env: EnvWithHandlerCfg::new_with_cfg_env(
                cfg_env,
                block_env.clone(),
                Default::default(),
            ),
            block_env,
        })
    }

    pub fn get_state_at_ticks(
        &self,
        address: Address,
        ticks: Vec<i32>,
    ) -> eyre::Result<Vec<(i32, UniswapV3::ticksReturn)>> {
        ticks
            .clone()
            .into_iter()
            .map(|tick| {
                let call = UniswapV3::ticksCall { _0: tick };
                let to = address;

                Ok((tick, self.transact(call, to)?))
            })
            .collect::<eyre::Result<Vec<_>>>()
    }

    pub fn get_tick_bitmaps(
        &self,
        address: Address,
        words: Range<i16>,
    ) -> eyre::Result<Vec<(i16, U256)>> {
        words
            .clone()
            .into_iter()
            .map(|word| {
                let call = UniswapV3::tickBitmapCall { _0: word };
                let to = address;

                Ok((word, self.transact(call, to)?._0))
            })
            .collect::<eyre::Result<Vec<_>>>()
    }

    pub fn get_tick_spacing(&self, to: Address) -> eyre::Result<i32> {
        let request = UniswapV3::tickSpacingCall {};
        Ok(self.transact(request, to)?._0)
    }

    fn transact<C: SolCall>(&self, call: C, to: Address) -> eyre::Result<C::Return> {
        let mut env = self.env.clone();
        env.tx = TxEnv {
            transact_to: TransactTo::Call(to),
            data: call.abi_encode().into(),
            chain_id: Some(1),
            gas_limit: self.block_env.gas_limit.min(U256::from(u64::MAX)).to(),
            gas_priority_fee: Some(U256::MAX),
            ..Default::default()
        };

        let mut cache_db = CacheDB::new(self.state_db.clone());

        let (res, _) = self.node.reth_api.transact(&mut cache_db, env)?;

        match res.result {
            reth_revm::primitives::ExecutionResult::Success {
                reason: _,
                gas_used: _,
                gas_refunded: _,
                logs: _,
                output,
            } => Ok(C::abi_decode_returns(&output.data(), false)?),
            reth_revm::primitives::ExecutionResult::Revert { .. } => {
                Err(eyre::ErrReport::msg("Revert"))
            }
            reth_revm::primitives::ExecutionResult::Halt {
                reason,
                gas_used: _,
            } => Err(eyre::ErrReport::msg(format!("HALT: {:?}", reason))),
        }
    }
}
