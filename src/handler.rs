use crate::ticks::TickFetcher;
use futures::StreamExt;
use futures::{stream::FuturesUnordered, Future};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::error;

pub struct PoolHandler {
    pub fetcher: TickFetcher,
    pub futs: FuturesUnordered<JoinHandle<Result<(), (u64, eyre::ErrReport)>>>,
    pub end_block: u64,
    pub handle: Handle,
    pub max_tasks: usize,
}

impl PoolHandler {
    pub fn new(fetcher: TickFetcher, end_block: u64, handle: Handle, max_tasks: usize) -> Self {
        Self {
            fetcher,
            futs: FuturesUnordered::new(),
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

        let mut work = 1024;
        loop {
            while this.futs.len() < this.max_tasks && this.end_block >= this.fetcher.current_block {
                this.futs.push(
                    this.handle
                        .clone()
                        .spawn(this.fetcher.clone().execute_block()),
                );
                this.fetcher.current_block += 1;
            }

            if let Poll::Ready(Some(val)) = this.futs.poll_next_unpin(cx) {
                if let Ok(Err((b, e))) = val {
                    error!(target: "uni-v3", "pool: {:?} - failed to get block {b}, retrying - {:?}", this.fetcher.pool, e);
                    let curr_block = this.fetcher.current_block;
                    this.fetcher.current_block = b;
                    this.futs.push(
                        this.handle
                            .clone()
                            .spawn(this.fetcher.clone().execute_block()),
                    );
                    this.fetcher.current_block = curr_block;
                }
            }

            if this.futs.is_empty() && this.end_block <= this.fetcher.current_block {
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
