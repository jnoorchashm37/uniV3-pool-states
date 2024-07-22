use crate::node::EthNodeApi;
use crate::pools::PoolCaller;
use futures::StreamExt;
use futures::{stream::FuturesUnordered, Future};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tracing::error;

use crate::pools::{types::PoolData, PoolFetcher};

pub struct PoolHandler {
    pub node: Arc<EthNodeApi>,
    pub db_tx: UnboundedSender<Vec<PoolData>>,
    pub pools: Vec<Arc<Box<dyn PoolFetcher>>>,
    pub futs: FuturesUnordered<JoinHandle<Result<usize, (u64, eyre::ErrReport)>>>,
    pub current_block: u64,
    pub end_block: u64,
    pub handle: Handle,
    pub active_tasks: usize,
    pub max_concurrent_tasks: usize,
}

impl PoolHandler {
    pub fn new(
        node: Arc<EthNodeApi>,
        db_tx: UnboundedSender<Vec<PoolData>>,
        pools: Vec<Arc<Box<dyn PoolFetcher>>>,
        start_block: u64,
        end_block: u64,
        handle: Handle,
        max_concurrent_tasks: usize,
    ) -> Self {
        Self {
            node,
            db_tx,
            pools,
            futs: FuturesUnordered::new(),
            current_block: start_block,
            end_block,
            handle,
            active_tasks: 0,
            max_concurrent_tasks,
        }
    }
}

impl Future for PoolHandler {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut work = 4096;

        loop {
            while let Poll::Ready(Some(val)) = this.futs.poll_next_unpin(cx) {
                match val {
                    Ok(Ok(t)) => this.active_tasks -= t,
                    Ok(Err((b, e))) => {
                        error!(target: "uniV3", "failed to get block {b}, retrying - {:?}", e);
                        let caller =
                            PoolCaller::new(this.node.clone(), this.db_tx.clone(), &this.pools, b);
                        this.futs
                            .push(this.handle.clone().spawn(caller.execute_block()));
                    }
                    _ => (),
                }
            }

            if this.end_block >= this.current_block
                && this.active_tasks <= this.max_concurrent_tasks
            {
                let caller = PoolCaller::new(
                    this.node.clone(),
                    this.db_tx.clone(),
                    &this.pools,
                    this.current_block,
                );
                this.active_tasks += caller.pools.len();
                this.futs
                    .push(this.handle.clone().spawn(caller.execute_block()));
                this.current_block += 1;
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
