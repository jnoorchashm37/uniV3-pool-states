use std::sync::Arc;

use alloy_primitives::Address;
use alloy_primitives::U256;
use futures::future::join_all;

use crate::node::RethDbApiClient;

#[derive(Clone)]
pub struct TickFetcher {
    node: Arc<RethDbApiClient>,
    pool: Address,
    min_word: i16,
    max_word: i16,
    tick_spacing: i32,
}

impl TickFetcher {
    pub async fn new(node: Arc<RethDbApiClient>, pool: Address) -> eyre::Result<Self> {
        let tick_spacing = node.get_tick_spacing(pool, None).await?;

        let min_word = tick_to_word(-887272, tick_spacing) as i16;
        let max_word = tick_to_word(887272, tick_spacing) as i16;

        Ok(Self {
            node,
            pool,
            min_word,
            max_word,
            tick_spacing,
        })
    }

    pub async fn execute_block(&self, block_number: u64) -> eyre::Result<Vec<i32>> {
        let bitmaps = self.get_bitmaps(block_number).await?;

        let vals = bitmaps
            .into_iter()
            .flat_map(|(idx, map)| {
                if map != U256::ZERO {
                    (0..256)
                        .into_iter()
                        .filter_map(|i| {
                            if (map & (U256::from(1u8) << i)) != U256::ZERO {
                                let tick_index = (idx as i32 * 256 + i) * self.tick_spacing;
                                Some(tick_index)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<_>>();

        Ok(vals)
    }

    async fn get_bitmaps(&self, block_number: u64) -> eyre::Result<Vec<(i16, U256)>> {
        join_all(
            (self.min_word..self.max_word)
                .into_iter()
                .map(|i| async move {
                    let bitmap_result = self
                        .node
                        .get_tick_bitmap(self.pool, i, Some(block_number))
                        .await?;
                    Ok((i, bitmap_result))
                }),
        )
        .await
        .into_iter()
        .collect::<eyre::Result<Vec<_>>>()
    }
}

fn tick_to_word(tick: i32, tick_spacing: i32) -> i32 {
    let mut compressed = tick / tick_spacing;
    if tick < 0 && tick % tick_spacing != 0 {
        compressed -= 1;
    }
    tick >> 8
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_map() {
        dotenv::dotenv().ok();

        let db_path = std::env::var("RETH_DB_PATH").expect("no 'RETH_DB_PATH' in .env");
        let node = RethDbApiClient::new(&db_path, tokio::runtime::Handle::current())
            .await
            .unwrap();

        let fetcher = TickFetcher::new(
            Arc::new(node),
            Address::from_str("0xCBCdF9626bC03E24f779434178A73a0B4bad62eD").unwrap(),
        )
        .await
        .unwrap();

        let ticks = fetcher.execute_block(19858960).await.unwrap();

        for t in ticks {
            println!("{t}");
        }
    }

    #[test]
    fn t() {
        let min_word = tick_to_word(-887272, 60);
        let max_word = tick_to_word(887272, 60);

        println!("MIN: {}", min_word);
        println!("MAX: {}", max_word);
    }
}
