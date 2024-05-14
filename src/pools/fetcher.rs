use crate::{execute_on_threadpool, node::RethDbApiClient};
use alloy_primitives::Address;
use alloy_sol_types::SolCall;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reth_primitives::{
    revm::env::tx_env_with_recovered, TransactionSignedEcRecovered, TxHash, U256,
};
use reth_provider::StateProvider;
use reth_revm::{
    database::StateProviderDatabase,
    db::CacheDB,
    primitives::{BlockEnv, CfgEnvWithHandlerCfg, EnvWithHandlerCfg, TransactTo, TxEnv},
    DatabaseCommit,
};
use reth_rpc::eth::EthTransactions;
use std::{collections::HashSet, ops::Range, sync::Arc};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info};

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
        let parent_block_txs = self
            .node
            .get_parent_block_with_signers(self.block_number)
            .await?
            .into_transactions_ecrecovered()
            .collect::<Vec<_>>();

        let addresses = self
            .pools
            .iter()
            .map(|pool| pool.pool_address)
            .collect::<Vec<_>>();

        let pool_txs = self
            .node
            .get_transaction_traces_with_addresses(&&addresses, self.block_number)
            .await?;

        let state = execute_on_threadpool(|| {
            self.run_cycle(pool_inner, &parent_block_txs, &pool_txs, &self.pools)
        })?;
        info!(target: "uni-v3::fetcher", "completed block {} for {} pools with {} total ticks", self.block_number,self.pools.len(), state.len());

        self.db_tx.send(state)?;

        Ok(())
    }

    fn run_cycle(
        &self,
        inner: PoolDBInner,
        parent_block_txs: &[TransactionSignedEcRecovered],
        pool_txs: &[(Address, TxHash)],
        pools: &[&PoolTickFetcher],
    ) -> eyre::Result<Vec<PoolState>> {
        let state = pools
            .par_iter()
            .map(|pool| {
                let inner = inner.clone();
                inner.execute_cycle(
                    self.block_number,
                    &parent_block_txs,
                    pool.pool_address,
                    pool_txs,
                    |db_inner, tx, bn| pool.execute_block(db_inner, tx, bn),
                )
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
    pub state_db: CacheDB<Arc<StateProviderDatabase<Box<dyn StateProvider>>>>,
    pub cfg: CfgEnvWithHandlerCfg,
    pub env: EnvWithHandlerCfg,
    pub block_env: BlockEnv,
}

impl PoolDBInner {
    pub async fn new(node: Arc<RethDbApiClient>, block_number: u64) -> eyre::Result<Self> {
        let parent_block = block_number - 1;
        let state_db = node.state_provider_db(parent_block)?;
        let (cfg_env, block_env, _) = node.get_evm_env_at(block_number).await?;

        Ok(Self {
            node,
            state_db: CacheDB::new(Arc::new(state_db)),
            cfg: cfg_env.clone(),
            env: EnvWithHandlerCfg::new_with_cfg_env(
                cfg_env,
                block_env.clone(),
                Default::default(),
            ),
            block_env,
        })
    }

    pub fn get_state_at_ticks(
        &mut self,
        address: Address,
        ticks: Vec<i32>,
    ) -> eyre::Result<Vec<(i32, UniswapV3::ticksReturn)>> {
        ticks
            .clone()
            .into_iter()
            .map(|tick| {
                let call = UniswapV3::ticksCall { _0: tick };
                let to = address;

                Ok((tick, self.transact_call(call, to)?))
            })
            .collect::<eyre::Result<Vec<_>>>()
    }

    pub fn get_tick_bitmaps(
        &mut self,
        address: Address,
        words: Range<i16>,
    ) -> eyre::Result<Vec<(i16, U256)>> {
        words
            .clone()
            .into_iter()
            .map(|word| {
                let call = UniswapV3::tickBitmapCall { _0: word };
                let to = address;

                Ok((word, self.transact_call(call, to)?._0))
            })
            .collect::<eyre::Result<Vec<_>>>()
    }

    pub fn get_tick_spacing(&mut self, to: Address) -> eyre::Result<i32> {
        let request = UniswapV3::tickSpacingCall {};
        Ok(self.transact_call(request, to)?._0)
    }

    fn transact_call<C: SolCall>(&mut self, call: C, to: Address) -> eyre::Result<C::Return> {
        let mut env = self.env.clone();
        env.tx = TxEnv {
            transact_to: TransactTo::Call(to),
            data: call.abi_encode().into(),
            chain_id: Some(1),
            gas_limit: self.block_env.gas_limit.min(U256::from(u64::MAX)).to(),
            ..Default::default()
        };

        let (res, _) = self.node.reth_api.transact(&mut self.state_db, env)?;

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

    fn execute_cycle<F>(
        mut self,
        block_number: u64,
        parent_block_txs: &[TransactionSignedEcRecovered],
        pool_address: Address,
        pool_txs: &[(Address, TxHash)],
        f: F,
    ) -> eyre::Result<Vec<PoolState>>
    where
        F: Fn(&mut PoolDBInner, TxHash, u64) -> eyre::Result<Vec<PoolState>>,
    {
        let pool_txs = pool_txs
            .iter()
            .filter(|(p, _)| p == &pool_address)
            .map(|(_, t)| t)
            .collect::<HashSet<_>>();

        let pool_states = parent_block_txs
            .iter()
            .map(|transaction| {
                let tx = tx_env_with_recovered(transaction);
                println!("TX: {:?}", tx);
                let env = EnvWithHandlerCfg::new_with_cfg_env(
                    self.cfg.clone(),
                    self.block_env.clone(),
                    tx,
                );
                println!("\n\nENV: {:?}\n\n\n\n", env);
                let (res, _) = self.node.reth_api.transact(&mut self.state_db, env)?;
                self.state_db.commit(res.state);

                if let Some(pool_tx) = pool_txs.get(&transaction.hash) {
                    Ok(f(&mut self, **pool_tx, block_number)?)
                } else {
                    Ok(Vec::new())
                }
            })
            .collect::<eyre::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        debug!(target: "uni-v3::fetcher", "completed block {} for pool {} with {} total ticks", block_number,pool_address, pool_states.len());

        Ok(pool_states)
    }
}
