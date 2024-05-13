use crate::db::UniswapV3Tables;
use crate::node::RethDbApiClient;
use crate::pools::PoolCaller;
use crate::ticks::TickFetcher;
use db_interfaces::clickhouse::client::ClickhouseClient;
use futures::StreamExt;
use futures::{stream::FuturesUnordered, Future};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::error;

pub struct PoolHandler {
    pub node: Arc<RethDbApiClient>,
    pub db: &'static ClickhouseClient<UniswapV3Tables>,
    pub pools: &'static [TickFetcher],
    pub futs: FuturesUnordered<JoinHandle<Result<(), (u64, eyre::ErrReport)>>>,
    pub current_block: u64,
    pub end_block: u64,
    pub handle: Handle,
    pub max_tasks: usize,
}

impl PoolHandler {
    pub fn new(
        node: Arc<RethDbApiClient>,
        db: &'static ClickhouseClient<UniswapV3Tables>,
        pools: &'static [TickFetcher],
        start_block: u64,
        end_block: u64,
        handle: Handle,
        max_tasks: usize,
    ) -> Self {
        Self {
            node,
            db,
            pools,
            futs: FuturesUnordered::new(),
            current_block: start_block,
            end_block,
            handle,
            max_tasks,
        }
    }
}

impl Future for PoolHandler {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut work = 4096;

        loop {
            // while this.futs.len() < this.max_tasks && this.end_block >= this.current_block {
            if this.end_block >= this.current_block {
                let caller =
                    PoolCaller::new(this.node.clone(), this.db, this.pools, this.current_block);
                let this_handle = this.handle.clone();
                this.futs.push(
                    this.handle
                        .clone()
                        .spawn_blocking(move || this_handle.block_on(caller.execute_block())),
                );
                this.current_block += 1;
            }

            if let Poll::Ready(Some(val)) = this.futs.poll_next_unpin(cx) {
                if let Ok(Err((b, e))) = val {
                    error!(target: "uni-v3", "failed to get block {b}, retrying - {:?}", e);
                    let caller = PoolCaller::new(this.node.clone(), this.db, this.pools, b);
                    let this_handle = this.handle.clone();
                    this.futs.push(
                        this.handle
                            .clone()
                            .spawn_blocking(move || this_handle.block_on(caller.execute_block())),
                    );
                }
            }

            if this.futs.is_empty() && this.end_block <= this.current_block {
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
