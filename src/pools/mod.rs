mod contracts;
pub use contracts::*;

mod fetcher;
pub use fetcher::*;

mod state;
pub use state::*;

mod ticks;
pub use ticks::*;

mod slot0;
pub use slot0::*;

pub trait PoolFetcher: Send + Sync {
    fn execute_block(
        &self,
        inner: &mut PoolDBInner,
        block_number: u64,
        tx_hash: alloy_primitives::TxHash,
        tx_index: u64,
    ) -> eyre::Result<PoolData>;

    fn earliest_block(&self) -> u64;

    fn pool_address(&self) -> alloy_primitives::Address;
}
