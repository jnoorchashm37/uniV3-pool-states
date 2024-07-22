mod contracts;
pub use contracts::*;

mod fetcher;
pub use fetcher::*;

pub mod types;

mod ticks;
pub use ticks::*;

mod slot0;
pub use slot0::*;

mod trades;
pub use trades::*;

pub trait PoolFetcher: Send + Sync {
    fn is_re_executed(&self) -> bool;
    fn is_decoded(&self) -> bool;

    fn re_execute_block(
        &self,
        _inner: &mut PoolDBInner,
        _block_number: u64,
        _tx_hash: alloy_primitives::TxHash,
        _tx_index: u64,
    ) -> eyre::Result<Vec<crate::pools::types::PoolData>> {
        unreachable!()
    }

    fn decode_block(
        &self,
        _block_number: u64,
        _tx_calls: &[crate::node::FilteredTraceCall],
    ) -> eyre::Result<Vec<crate::pools::types::PoolData>> {
        unreachable!()
    }

    fn earliest_block(&self) -> u64;

    fn pool_address(&self) -> alloy_primitives::Address;
}
