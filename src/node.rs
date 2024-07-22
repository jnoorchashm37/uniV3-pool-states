use alloy_primitives::Address;
use alloy_primitives::TxHash;
use alloy_rpc_types::BlockId;
use alloy_rpc_types_trace::parity::Action;
use alloy_rpc_types_trace::parity::TraceOutput;
use alloy_rpc_types_trace::parity::TraceResultsWithTransactionHash;
use alloy_rpc_types_trace::parity::TraceType;

use reth_api_libmdbx::RethDbApiClient;
use reth_primitives::Bytes;
use reth_primitives::SealedBlockWithSenders;
use reth_provider::StateProvider;
use reth_revm::{
    database::StateProviderDatabase,
    primitives::{BlockEnv, CfgEnvWithHandlerCfg},
};
use reth_rpc::eth::EthTransactions;
use reth_rpc_api::EthApiServer;
use tracing::info;

use std::collections::HashSet;
use tokio::runtime::Handle;

pub struct EthNodeApi {
    pub reth_api: RethDbApiClient,
}

impl EthNodeApi {
    pub fn new(db_path: &str, handle: Handle) -> eyre::Result<Self> {
        info!(target: "uniV3", "spawned eth node connection");
        Ok(Self {
            reth_api: RethDbApiClient::new(db_path, handle)?,
        })
    }

    pub fn get_current_block(&self) -> eyre::Result<u64> {
        Ok(self.reth_api.eth_api.block_number()?.to())
    }

    pub async fn get_evm_env_at(
        &self,
        block_number: u64,
    ) -> eyre::Result<(CfgEnvWithHandlerCfg, BlockEnv, BlockId)> {
        Ok(self
            .reth_api
            .eth_api
            .evm_env_at(block_number.into())
            .await?)
    }

    pub fn state_provider_db(
        &self,
        block_number: u64,
    ) -> eyre::Result<StateProviderDatabase<Box<dyn StateProvider>>> {
        let state_provider = self
            .reth_api
            .eth_api
            .state_at_block_id(block_number.into())?;
        Ok(StateProviderDatabase::new(state_provider))
    }

    pub async fn get_block_with_signers(
        &self,
        block_number: u64,
    ) -> eyre::Result<SealedBlockWithSenders> {
        let block = self
            .reth_api
            .eth_api
            .block_by_id_with_senders(block_number.into())
            .await?
            .ok_or(eyre::ErrReport::msg(format!(
                "no sealed block found for block {block_number}"
            )))?;

        Ok(block)
    }

    pub async fn get_transaction_traces(
        &self,
        block_number: u64,
    ) -> eyre::Result<Vec<TraceResultsWithTransactionHash>> {
        Ok(self
            .reth_api
            .trace
            .replay_block_transactions(block_number.into(), HashSet::from([TraceType::Trace]))
            .await?
            .ok_or(eyre::ErrReport::msg(format!(
                "no traces found for block {block_number}"
            )))?)
    }

    pub async fn get_filtered_transaction_traces<F, O>(
        &self,
        block_number: u64,
        f: F,
    ) -> eyre::Result<Vec<O>>
    where
        F: Fn(TraceResultsWithTransactionHash) -> Vec<O>,
    {
        let traces = self.get_transaction_traces(block_number).await?;

        let vals = traces.into_iter().flat_map(|tx| f(tx)).collect::<Vec<_>>();

        Ok(vals)
    }
}

pub fn filter_traces_by_address_set_to_tx_hash(
    tx: TraceResultsWithTransactionHash,
    addresses: &[Address],
) -> Vec<(Address, TxHash)> {
    let address_set = addresses.iter().map(|a| *a).collect::<HashSet<_>>();
    tx.full_trace
        .trace
        .into_iter()
        .filter_map(|trace| match trace.action {
            Action::Call(call) => {
                if let Some(f) = address_set.get(&call.from) {
                    Some((*f, tx.transaction_hash))
                } else if let Some(t) = address_set.get(&call.to) {
                    Some((*t, tx.transaction_hash))
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect::<Vec<_>>()
}

pub fn filter_traces_by_address_to_call_input(
    tx: TraceResultsWithTransactionHash,
    addresses: &[Address],
) -> Vec<(Address, FilteredTraceCall)> {
    let address_set = addresses.iter().map(|a| *a).collect::<HashSet<_>>();
    let mut failed = false;
    let traces = tx
        .full_trace
        .trace
        .into_iter()
        .filter_map(|trace| {
            if trace.error.is_some() {
                failed = true;
                return None;
            }

            match trace.action {
                Action::Call(call) => {
                    if let Some(f) = address_set.get(&call.to) {
                        if let Some(ret) = trace.result {
                            match ret {
                                TraceOutput::Call(call_ret) => Some((
                                    *f,
                                    FilteredTraceCall::new(
                                        tx.transaction_hash,
                                        call.input,
                                        call_ret.output,
                                    ),
                                )),

                                _ => None,
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .collect();

    if failed {
        Vec::new()
    } else {
        traces
    }
}

pub struct FilteredTraceCall {
    pub tx_hash: TxHash,
    pub func_sig: [u8; 4],
    pub input: Bytes,
    pub output: Bytes,
}

impl FilteredTraceCall {
    fn new(tx_hash: TxHash, input: Bytes, output: Bytes) -> Self {
        Self {
            tx_hash,
            func_sig: input[..4].try_into().unwrap(),
            input,
            output,
        }
    }
}
