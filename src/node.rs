use std::{fmt::Debug, ops::Range, path::Path, sync::Arc};

use alloy_primitives::Address;
use alloy_rpc_types::TransactionRequest;
use alloy_sol_types::SolCall;

use eyre::Context;
use itertools::Itertools;
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
use reth_primitives::{constants::ETHEREUM_BLOCK_GAS_LIMIT, MAINNET, U256};
use reth_provider::{providers::BlockchainProvider, DatabaseProvider, ProviderFactory};
use reth_revm::EvmProcessorFactory;
use reth_rpc::{
    eth::{
        cache::{EthStateCache, EthStateCacheConfig},
        gas_oracle::{GasPriceOracle, GasPriceOracleConfig},
        EthFilterConfig, FeeHistoryCache, FeeHistoryCacheConfig,
    },
    DebugApi, EthApi, EthFilter, TraceApi,
};
use reth_rpc_api::EthApiServer;
use reth_rpc_types::{Bundle, StateContext, TransactionInput};
use reth_tasks::{
    pool::{BlockingTaskGuard, BlockingTaskPool},
    TaskManager,
};
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, CoinbaseTipOrdering, EthPooledTransaction,
    EthTransactionValidator, Pool, TransactionValidationTaskExecutor,
};
use tokio::runtime::Handle;

use crate::contracts::UniswapV3::{self};

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
    reth_api: RethApi,
}

impl RethDbApiClient {
    pub async fn new(db_path: &str, handle: Handle) -> eyre::Result<Self> {
        let (reth_api, _, _, _, db) = init(Path::new(db_path), handle)?;

        Ok(Self { reth_api })
    }

    pub fn get_current_block(&self) -> eyre::Result<u64> {
        Ok(self.reth_api.block_number()?.to())
    }

    pub async fn get_tick_spacing(
        &self,
        address: Address,
        block_number: Option<u64>,
    ) -> eyre::Result<i32> {
        let request = UniswapV3::tickSpacingCall {};
        Ok(self
            .make_call_request(request, address, block_number)
            .await?
            ._0)
    }

    pub async fn get_tick_bitmaps(
        &self,
        address: Address,
        words: Range<i16>,
        block_number: u64,
    ) -> eyre::Result<Vec<(i16, U256)>> {
        let requests = words
            .clone()
            .into_iter()
            .map(|word| UniswapV3::tickBitmapCall { _0: word })
            .collect();

        Ok(self
            .make_call_many_request(requests, address, Some(block_number))
            .await?
            .into_iter()
            .zip(words)
            .map(|(v, t)| (t, v._0))
            .collect_vec())
    }

    pub async fn get_state_at_ticks(
        &self,
        address: Address,
        ticks: Vec<i32>,
        block_number: u64,
    ) -> eyre::Result<Vec<(i32, UniswapV3::ticksReturn)>> {
        let requests = ticks
            .clone()
            .into_iter()
            .map(|tick| UniswapV3::ticksCall { _0: tick })
            .collect();

        Ok(self
            .make_call_many_request(requests, address, Some(block_number))
            .await?
            .into_iter()
            .zip(ticks)
            .map(|(v, t)| (t, v))
            .collect_vec())
    }

    async fn make_call_request<C: SolCall>(
        &self,
        call: C,
        to: Address,
        block_number: Option<u64>,
    ) -> eyre::Result<C::Return> {
        let encoded = call.abi_encode();
        let req = TransactionRequest {
            to: Some(to),
            input: TransactionInput::new(encoded.into()),
            chain_id: Some(1),
            ..Default::default()
        };

        let res = self
            .reth_api
            .call(req, block_number.map(Into::into), Default::default())
            .await?;

        Ok(C::abi_decode_returns(&res, false)?)
    }

    async fn make_call_many_request<C: SolCall>(
        &self,
        calls: Vec<C>,
        to: Address,
        block_number: Option<u64>,
    ) -> eyre::Result<Vec<C::Return>> {
        let reqs = calls
            .into_iter()
            .map(|call| {
                let encoded = call.abi_encode();
                TransactionRequest {
                    to: Some(to),
                    input: TransactionInput::new(encoded.into()),
                    chain_id: Some(1),
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();

        let bundle = Bundle {
            transactions: reqs,
            block_override: None,
        };

        let state = StateContext {
            block_number: block_number.map(Into::into),
            transaction_index: None,
        };

        let res = self
            .reth_api
            .call_many(bundle, Some(state), Default::default())
            .await?
            .into_iter()
            .filter_map(|r| r.value.map(|v| Ok(C::abi_decode_returns(&v, false)?)))
            .collect::<eyre::Result<Vec<_>>>();

        res
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
