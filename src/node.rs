use alloy_primitives::Address;
use alloy_primitives::TxHash;
use alloy_rpc_types::BlockId;
use alloy_rpc_types_trace::parity::Action;
use alloy_rpc_types_trace::parity::TraceType;
use reth_api_libmdbx::RethDbApiClient;
use reth_primitives::SealedBlockWithSenders;
use reth_provider::StateProvider;
use reth_revm::{
    database::StateProviderDatabase,
    primitives::{BlockEnv, CfgEnvWithHandlerCfg},
};
use reth_rpc::eth::EthTransactions;
use reth_rpc_api::EthApiServer;

use std::collections::HashSet;
use tokio::runtime::Handle;

pub struct EthNodeApi {
    pub reth_api: RethDbApiClient,
}

impl EthNodeApi {
    pub fn new(db_path: &str, handle: Handle) -> eyre::Result<Self> {
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

    pub async fn get_transaction_traces_with_addresses(
        &self,
        addresses: &[Address],
        block_number: u64,
    ) -> eyre::Result<Vec<(Address, TxHash)>> {
        let traces = self
            .reth_api
            .trace
            .replay_block_transactions(block_number.into(), HashSet::from([TraceType::Trace]))
            .await?
            .ok_or(eyre::ErrReport::msg(format!(
                "no traces found for block {block_number}"
            )))?;

        let address_set = addresses.iter().collect::<HashSet<_>>();

        let vals = traces
            .into_iter()
            .flat_map(|tx| {
                tx.full_trace
                    .trace
                    .into_iter()
                    .filter_map(|trace| match trace.action {
                        Action::Call(call) => {
                            if let Some(f) = address_set.get(&call.from) {
                                Some((**f, tx.transaction_hash))
                            } else if let Some(t) = address_set.get(&call.to) {
                                Some((**t, tx.transaction_hash))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Ok(vals)
    }
}
