use crate::{
    execute_on_threadpool,
    node::{
        filter_traces_by_address_set_to_tx_hash, filter_traces_by_address_to_call_input,
        EthNodeApi, FilteredTraceCall,
    },
};
use alloy_primitives::Address;
use alloy_sol_types::SolCall;
use itertools::Itertools;
use reth_primitives::revm::env::tx_env_with_recovered;

use super::{PoolFetcher, UniswapV3};
use crate::pools::types::PoolData;

use alloy_primitives::{TxHash, U256};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reth_primitives::TransactionSignedEcRecovered;
use reth_provider::StateProvider;
use reth_revm::{
    database::StateProviderDatabase,
    db::CacheDB,
    primitives::{BlockEnv, CfgEnvWithHandlerCfg, EnvWithHandlerCfg, TransactTo, TxEnv},
    DatabaseCommit,
};
use reth_rpc::eth::EthTransactions;
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    sync::Arc,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info};

pub struct PoolCaller {
    pub node: Arc<EthNodeApi>,
    pub db_tx: UnboundedSender<Vec<PoolData>>,
    pub pools: Vec<Arc<Box<dyn PoolFetcher>>>,
    pub block_number: u64,
}

impl PoolCaller {
    pub fn new(
        node: Arc<EthNodeApi>,
        db_tx: UnboundedSender<Vec<PoolData>>,
        pools: &[Arc<Box<dyn PoolFetcher>>],
        block_number: u64,
    ) -> Self {
        let pools = pools
            .iter()
            .filter(|pool| pool.earliest_block() <= block_number)
            .cloned()
            .collect::<Vec<_>>();
        Self {
            node,
            db_tx,
            pools,
            block_number,
        }
    }

    pub async fn execute_block(self) -> Result<usize, (u64, eyre::ErrReport)> {
        let data = self.run_block().await.map_err(|e| (self.block_number, e))?;

        self.db_tx
            .send(data)
            .map_err(|e| (self.block_number, e.into()))?;

        Ok(self.pools.len())
    }

    async fn run_block(&self) -> eyre::Result<Vec<PoolData>> {
        let (re_executed, decoded) =
            tokio::try_join!(self.re_execute_block(), self.decode_block())?;

        Ok(re_executed.into_iter().chain(decoded).collect())
    }

    async fn decode_block(&self) -> eyre::Result<Vec<PoolData>> {
        let addresses = self
            .pools
            .iter()
            .filter(|pool| pool.is_decoded())
            .map(|pool| pool.pool_address())
            .collect::<Vec<_>>();

        let pool_txs = self
            .node
            .get_filtered_transaction_traces(self.block_number, |tx| {
                filter_traces_by_address_to_call_input(tx, &addresses)
            })
            .await?
            .into_iter()
            .into_group_map();

        if pool_txs.is_empty() {
            debug!(target: "uniV3::fetcher", "no transactions found in block {} for {} pools", self.block_number,self.pools.len());
            return Ok(Vec::new());
        }

        let state =
            execute_on_threadpool(|| self.decode_transactions(self.block_number, &pool_txs))?;
        info!(target: "uniV3::fetcher", "completed block {} for {} pools with {} total values", self.block_number, self.pools.len(), state.len());

        Ok(state)
    }

    async fn re_execute_block(&self) -> eyre::Result<Vec<PoolData>> {
        let pool_inner = PoolDBInner::new(self.node.clone(), self.block_number).await?;
        let parent_block_txs = self
            .node
            .get_block_with_signers(self.block_number)
            .await?
            .into_transactions_ecrecovered()
            .collect::<Vec<_>>();

        if parent_block_txs.is_empty() {
            debug!(target: "uniV3::fetcher", "no transactions found in block {} for {} pools", self.block_number,self.pools.len());
            return Ok(Vec::new());
        }

        let addresses = self
            .pools
            .iter()
            .filter(|pool| pool.is_re_executed())
            .map(|pool| pool.pool_address())
            .collect::<Vec<_>>();

        let pool_txs = self
            .node
            .get_filtered_transaction_traces(self.block_number, |tx| {
                filter_traces_by_address_set_to_tx_hash(tx, &addresses)
            })
            .await?;

        let state = execute_on_threadpool(|| {
            self.re_execute_transactions(pool_inner, &parent_block_txs, &pool_txs)
        })?;
        info!(target: "uniV3::fetcher", "completed block {} for {} pools with {} total values", self.block_number, self.pools.len(), state.len());

        Ok(state)
    }

    fn decode_transactions(
        &self,
        block_number: u64,
        block_txs: &HashMap<Address, Vec<FilteredTraceCall>>,
    ) -> eyre::Result<Vec<PoolData>> {
        let state = self
            .pools
            .par_iter()
            .filter(|pool| pool.is_decoded())
            .map(|pool| {
                let pool_txs = block_txs.get(&pool.pool_address()).unwrap();

                pool.decode_block(block_number, pool_txs)
            })
            .collect::<eyre::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(state)
    }

    fn re_execute_transactions(
        &self,
        inner: PoolDBInner,
        parent_block_txs: &[TransactionSignedEcRecovered],
        pool_txs: &[(Address, TxHash)],
    ) -> eyre::Result<Vec<PoolData>> {
        let state = self
            .pools
            .par_iter()
            .filter(|pool| pool.is_re_executed())
            .map(|pool| {
                let pool_txs = pool_txs
                    .iter()
                    .filter(|(p, _)| p == &pool.pool_address())
                    .map(|(_, t)| *t)
                    .collect::<HashSet<_>>();

                if pool_txs.is_empty() {
                    Ok(Vec::new())
                } else {
                    let inner = inner.clone();
                    inner.execute_cycle(
                        self.block_number,
                        parent_block_txs,
                        pool.pool_address(),
                        pool_txs,
                        |db_inner, bn, tx, tx_index| {
                            pool.re_execute_block(db_inner, bn, tx, tx_index)
                        },
                    )
                }
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
    pub node: Arc<EthNodeApi>,
    pub state_db: CacheDB<Arc<StateProviderDatabase<Box<dyn StateProvider>>>>,
    pub cfg: CfgEnvWithHandlerCfg,
    pub env: EnvWithHandlerCfg,
    pub block_env: BlockEnv,
}

impl PoolDBInner {
    pub async fn new(node: Arc<EthNodeApi>, block_number: u64) -> eyre::Result<Self> {
        let parent_block = block_number - 1;
        let state_db = node.state_provider_db(parent_block)?;
        let (cfg_env, mut block_env, _) = node.get_evm_env_at(block_number).await?;
        block_env.basefee = U256::ZERO;

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

    pub fn get_slot0(&mut self, to: Address) -> eyre::Result<UniswapV3::slot0Return> {
        let call = UniswapV3::slot0Call {};

        Ok(self.transact_call(call, to)?)
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

        let (res, _) = self
            .node
            .reth_api
            .eth_api
            .transact(&mut self.state_db, env)?;

        match res.result {
            reth_revm::primitives::ExecutionResult::Success {
                reason: _,
                gas_used: _,
                gas_refunded: _,
                logs: _,
                output,
            } => Ok(C::abi_decode_returns(output.data(), true)?),
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
        pool_txs: HashSet<TxHash>,
        f: F,
    ) -> eyre::Result<Vec<PoolData>>
    where
        F: Fn(&mut PoolDBInner, u64, TxHash, u64) -> eyre::Result<Vec<PoolData>>,
    {
        let pool_states = parent_block_txs
            .iter()
            .enumerate()
            .map(|(tx_index, transaction)| {
                let tx = tx_env_with_recovered(transaction);

                let env = EnvWithHandlerCfg::new_with_cfg_env(
                    self.cfg.clone(),
                    self.block_env.clone(),
                    tx,
                );

                if let Ok((res, _)) = self
                    .node
                    .reth_api
                    .eth_api
                    .transact(&mut self.state_db, env)
                    .map_err(|e| {
                        eyre::ErrReport::msg(format!("{:?} - {:?}", transaction.hash, e))
                    }) {
                        self.state_db.commit(res.state);

                        if res.result.is_success() {
                            if let Some(pool_tx) = pool_txs.get(&transaction.hash) {
                                return Ok(Some(f(&mut self, block_number, *pool_tx, tx_index as u64)?));
                            }
                        } else {
                            debug!(target: "uniV3::fetcher", "tx reverted in sim: {:?}", transaction.hash);
                        }
                    }



                Ok(None)
            })
            .collect::<eyre::Result<Vec<_>>>()?
            .into_iter()
            .flatten().flatten()
            .collect::<Vec<_>>();

        debug!(target: "uniV3::fetcher", "completed block {} for pool {} with {} total ticks", block_number,pool_address, pool_states.len());

        Ok(pool_states)
    }
}
