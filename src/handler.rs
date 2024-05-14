use crate::node::RethDbApiClient;
use crate::pools::{PoolCaller, PoolState, PoolTickFetcher};
use futures::StreamExt;
use futures::{stream::FuturesUnordered, Future};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tracing::error;

/// reth sets it's mdbx enviroment's max readers to 32000
/// we set ours slightly lower to account for errored blocks
const MDBX_READERS_LIMIT: usize = 20_000;

pub struct PoolHandler {
    pub node: Arc<RethDbApiClient>,
    pub db_tx: UnboundedSender<Vec<PoolState>>,
    pub pools: &'static [PoolTickFetcher],
    pub futs: FuturesUnordered<JoinHandle<Result<(), (u64, eyre::ErrReport)>>>,
    pub current_block: u64,
    pub end_block: u64,
    pub handle: Handle,
}

impl PoolHandler {
    pub fn new(
        node: Arc<RethDbApiClient>,
        db_tx: UnboundedSender<Vec<PoolState>>,
        pools: &'static [PoolTickFetcher],
        start_block: u64,
        end_block: u64,
        handle: Handle,
    ) -> Self {
        Self {
            node,
            db_tx,
            pools,
            futs: FuturesUnordered::new(),
            current_block: start_block,
            end_block,
            handle,
        }
    }
}

impl Future for PoolHandler {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut work = 4096;

        loop {
            if this.end_block >= this.current_block && this.futs.len() <= MDBX_READERS_LIMIT {
                let caller = PoolCaller::new(
                    this.node.clone(),
                    this.db_tx.clone(),
                    this.pools,
                    this.current_block,
                );
                this.futs
                    .push(this.handle.clone().spawn(caller.execute_block()));
                this.current_block += 1;
            }

            if let Poll::Ready(Some(val)) = this.futs.poll_next_unpin(cx) {
                if let Ok(Err((b, e))) = val {
                    error!(target: "uni-v3", "failed to get block {b}, retrying - {:?}", e);
                    let caller =
                        PoolCaller::new(this.node.clone(), this.db_tx.clone(), this.pools, b);
                    this.futs
                        .push(this.handle.clone().spawn(caller.execute_block()));
                }
            }

            if this.futs.is_empty() && this.end_block < this.current_block {
                return Poll::Ready(());
            }

            work -= 1;
            if work == 0 {
                cx.waker().wake_by_ref();
                break;
            }
        }

        Poll::Pending
    }
}
