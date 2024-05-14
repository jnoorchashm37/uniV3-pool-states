use std::{collections::HashSet, fmt::Debug, path::Path, sync::Arc};

use eyre::Context;

use reth_beacon_consensus::BeaconConsensus;
use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};
use reth_db::{
    database::Database,
    mdbx::{tx::Tx, DatabaseArguments, RO},
    models::client_version::ClientVersion,
    tables,
    transaction::DbTx,
    DatabaseEnv, DatabaseEnvKind,
};
use reth_network_api::noop::NoopNetwork;
use reth_node_ethereum::EthEvmConfig;
use reth_primitives::{
    constants::ETHEREUM_BLOCK_GAS_LIMIT, Address, SealedBlockWithSenders, TxHash, MAINNET,
};
use reth_provider::{
    providers::BlockchainProvider, DatabaseProvider, ProviderFactory, StateProvider,
};
use reth_revm::{
    database::StateProviderDatabase,
    primitives::{BlockEnv, CfgEnvWithHandlerCfg},
    EvmProcessorFactory,
};
use reth_rpc::{
    eth::{
        cache::{EthStateCache, EthStateCacheConfig},
        gas_oracle::{GasPriceOracle, GasPriceOracleConfig},
        EthFilterConfig, EthTransactions, FeeHistoryCache, FeeHistoryCacheConfig,
    },
    DebugApi, EthApi, EthFilter, TraceApi,
};
use reth_rpc_api::EthApiServer;
use reth_rpc_types::{
    trace::parity::{Action, TraceType},
    BlockId,
};
use reth_tasks::{
    pool::{BlockingTaskGuard, BlockingTaskPool},
    TaskManager,
};
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, CoinbaseTipOrdering, EthPooledTransaction,
    EthTransactionValidator, Pool, TransactionValidationTaskExecutor,
};
use tokio::runtime::Handle;

pub(crate) type RethClient = BlockchainProvider<
    Arc<DatabaseEnv>,
    ShareableBlockchainTree<Arc<DatabaseEnv>, EvmProcessorFactory<EthEvmConfig>>,
>;

pub(crate) type RethTxPool = Pool<
    TransactionValidationTaskExecutor<EthTransactionValidator<RethClient, EthPooledTransaction>>,
    CoinbaseTipOrdering<EthPooledTransaction>,
    InMemoryBlobStore,
>;

pub(crate) type RethApi = EthApi<RethClient, RethTxPool, NoopNetwork, EthEvmConfig>;
pub(crate) type RethFilter = EthFilter<RethClient, RethTxPool>;
pub(crate) type RethTrace = TraceApi<RethClient, RethApi>;
pub(crate) type RethDebug = DebugApi<RethClient, RethApi>;
pub(crate) type RethDbProvider = DatabaseProvider<Tx<RO>>;

pub struct RethDbApiClient {
    pub reth_api: RethApi,
    pub reth_trace: RethTrace,
}

impl RethDbApiClient {
    pub async fn new(db_path: &str, handle: Handle) -> eyre::Result<Self> {
        let (reth_api, _, reth_trace, _, _) = init(Path::new(db_path), handle)?;

        Ok(Self {
            reth_api,
            reth_trace,
        })
    }

    pub fn get_current_block(&self) -> eyre::Result<u64> {
        Ok(self.reth_api.block_number()?.to())
    }

    pub async fn get_evm_env_at(
        &self,
        block_number: u64,
    ) -> eyre::Result<(CfgEnvWithHandlerCfg, BlockEnv, BlockId)> {
        Ok(self.reth_api.evm_env_at(block_number.into()).await?)
    }

    pub fn state_provider_db(
        &self,
        block_number: u64,
    ) -> eyre::Result<StateProviderDatabase<Box<dyn StateProvider>>> {
        let state_provider = self.reth_api.state_at_block_id(block_number.into())?;
        Ok(StateProviderDatabase::new(state_provider))
    }

    pub async fn get_parent_block_with_signers(
        &self,
        block_number: u64,
    ) -> eyre::Result<SealedBlockWithSenders> {
        let block = self
            .reth_api
            .block_by_id_with_senders((block_number - 1).into())
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
            .reth_trace
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

fn init(
    db_path: &Path,
    handle: Handle,
) -> eyre::Result<(RethApi, RethFilter, RethTrace, RethDebug, RethDbProvider)> {
    let task_manager = TaskManager::new(handle.clone());
    let task_executor = task_manager.executor();

    handle.spawn(task_manager);

    let db = Arc::new(init_db(db_path).unwrap());
    let mut static_files_path = db_path.to_path_buf();
    static_files_path.pop();
    static_files_path.push("static_files");

    let chain_spec = MAINNET.clone();
    let provider_factory =
        ProviderFactory::new(Arc::clone(&db), chain_spec.clone(), static_files_path)?;

    let tree_externals = TreeExternals::new(
        provider_factory.clone(),
        Arc::new(BeaconConsensus::new(chain_spec.clone())),
        EvmProcessorFactory::new(chain_spec.clone(), EthEvmConfig::default()),
    );

    let tree_config = BlockchainTreeConfig::default();

    let blockchain_tree =
        ShareableBlockchainTree::new(BlockchainTree::new(tree_externals, tree_config, None)?);

    let provider = BlockchainProvider::new(provider_factory.clone(), blockchain_tree)?;

    let db_provider = provider_factory.clone().provider()?;

    let state_cache = EthStateCache::spawn(
        provider.clone(),
        EthStateCacheConfig::default(),
        EthEvmConfig::default(),
    );

    let blob_store = InMemoryBlobStore::default();
    let tx_pool = reth_transaction_pool::Pool::eth_pool(
        TransactionValidationTaskExecutor::eth(
            provider.clone(),
            chain_spec.clone(),
            blob_store.clone(),
            task_executor.clone(),
        ),
        blob_store,
        Default::default(),
    );

    let reth_api = EthApi::new(
        provider.clone(),
        tx_pool.clone(),
        Default::default(),
        state_cache.clone(),
        GasPriceOracle::new(
            provider.clone(),
            GasPriceOracleConfig::default(),
            state_cache.clone(),
        ),
        ETHEREUM_BLOCK_GAS_LIMIT,
        BlockingTaskPool::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(5)
                .build()
                .unwrap(),
        ),
        FeeHistoryCache::new(state_cache.clone(), FeeHistoryCacheConfig::default()),
        EthEvmConfig::default(),
        None,
    );

    let blocking_task_guard = BlockingTaskGuard::new(10);

    let reth_trace = TraceApi::new(
        provider.clone(),
        reth_api.clone(),
        blocking_task_guard.clone(),
    );

    let reth_debug = DebugApi::new(provider.clone(), reth_api.clone(), blocking_task_guard);

    let reth_filter = EthFilter::new(
        provider,
        tx_pool,
        state_cache,
        EthFilterConfig::default(),
        Box::new(task_executor),
    );

    Ok((reth_api, reth_filter, reth_trace, reth_debug, db_provider))
}

/// Opens up an existing database at the specified path.
fn init_db<P: AsRef<Path> + Debug>(path: P) -> eyre::Result<DatabaseEnv> {
    let _ = std::fs::create_dir_all(path.as_ref());
    let db = DatabaseEnv::open(
        path.as_ref(),
        DatabaseEnvKind::RO,
        DatabaseArguments::new(ClientVersion::default()),
    )?;

    view(&db, |tx| {
        for table in tables::Tables::ALL.iter().map(|table| table.name()) {
            tx.inner
                .open_db(Some(table))
                .wrap_err("Could not open db.")
                .unwrap();
        }
    })?;

    Ok(db)
}

/// allows for a function to be passed in through a RO libmdbx transaction
fn view<F, T>(db: &DatabaseEnv, f: F) -> eyre::Result<T>
where
    F: FnOnce(&<DatabaseEnv as Database>::TX) -> T,
{
    let tx = db.tx()?;
    let res = f(&tx);
    tx.commit()?;

    Ok(res)
}
