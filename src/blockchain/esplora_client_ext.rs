use crate::wallet::WalletScanUpdate;
use bdk_core::{BlockId, Update};
use bitcoin::{BlockHash, Script};
use esplora_client::Error;
use std::collections::BTreeMap;

pub trait BlockingClientExt {
    fn keychain_scan(
        &self,
        scripts: impl Iterator<Item = (u32, Script)> + Clone,
        stop_gap: usize,
        existing_chain: &BTreeMap<u32, BlockHash>,
        parallel_requests: usize,
    ) -> Result<(Option<u32>, Update), Error>;

    fn wallet_scan<K, I>(
        &self,
        keychains: BTreeMap<K, I>,
        stop_gap: usize,
        existing_chain: &BTreeMap<u32, BlockHash>,
        parallel_requests: usize,
    ) -> Result<WalletScanUpdate<K>, Error>
    where
        I: Iterator<Item = (u32, Script)> + Clone,
        K: Ord + Clone;
}

fn main_scan_loop(
    client: &esplora_client::BlockingClient,
    update: &mut Update,
    mut spks: impl Iterator<Item = (u32, Script)>,
    stop_gap: usize,
    parallel_requests: usize,
) -> Result<Option<u32>, Error> {
    let mut empty_scripts = 0;
    let mut last_active_index = None;
    loop {
        let handles = (0..parallel_requests)
            .filter_map(|_| {
                let (index, script) = spks.next()?;
                let client = client.clone();
                Some(std::thread::spawn(move || {
                    let mut related_txs = client.scripthash_txs(&script, None)?;
                    let n_confirmed = related_txs.iter().filter(|tx| tx.status.confirmed).count();
                    // esplora pages on 25 confirmed transactions. If there's 25 or more we
                    // keep requesting to see if there's more.
                    if n_confirmed >= 25 {
                        loop {
                            let new_related_txs = client
                                .scripthash_txs(&script, Some(related_txs.last().unwrap().txid))?;
                            let n = new_related_txs.len();
                            related_txs.extend(new_related_txs);
                            // we've reached the end
                            if n < 25 {
                                break;
                            }
                        }
                    }

                    Result::<_, Error>::Ok((index, related_txs))
                }))
            })
            .collect::<Vec<_>>();

        let n_handles = handles.len();

        for handle in handles {
            let (index, related_txs) = handle.join().unwrap()?; // TODO: don't unwrap
            if related_txs.is_empty() {
                empty_scripts += 1;
            } else {
                last_active_index = Some(index);
                empty_scripts = 0;
            }
            for tx in related_txs {
                update.insert_tx(tx.to_tx(), tx.status.block_height.into());
            }
        }

        if n_handles == 0 || empty_scripts >= stop_gap {
            break;
        }
    }

    Ok(last_active_index)
}

impl BlockingClientExt for esplora_client::BlockingClient {
    fn keychain_scan(
        &self,
        mut scripts: impl Iterator<Item = (u32, Script)> + Clone,
        stop_gap: usize,
        existing_chain: &BTreeMap<u32, BlockHash>,
        parallel_requests: usize,
    ) -> Result<(Option<u32>, Update), Error> {
        let mut update = Update::default();
        let backup_scripts = scripts.clone();

        let tip_at_start = {
            let height = self.get_height()?;
            let hash = self.get_block_hash(height)?;
            BlockId { height, hash }
        };

        update.insert_checkpoint(tip_at_start.height, tip_at_start.hash);

        let last_active_index =
            main_scan_loop(self, &mut update, scripts, stop_gap, parallel_requests)?;

        let blocks_at_end = self
            .get_blocks(None)?
            .into_iter()
            .map(|block| BlockId {
                hash: block.id,
                height: block.time.height,
            })
            .collect::<Vec<_>>();

        if !blocks_at_end.contains(&tip_at_start) {
            return self.keychain_scan(backup_scripts, stop_gap, existing_chain, parallel_requests);
        }

        for block in blocks_at_end {
            update.insert_checkpoint(block.height, block.hash);
        }

        Ok((last_active_index, update))
    }

    fn wallet_scan<K: Ord + Clone, I>(
        &self,
        keychains: BTreeMap<K, I>,
        stop_gap: usize,
        existing_chain: &BTreeMap<u32, BlockHash>,
        parallel_requests: usize,
    ) -> Result<WalletScanUpdate<K>, Error>
    where
        I: Iterator<Item = (u32, Script)> + Clone,
    {
        let mut wallet_scan = WalletScanUpdate::default();
        let backup_spks = keychains.clone();

        let tip_at_start = {
            let height = self.get_height()?;
            let hash = self.get_block_hash(height)?;
            BlockId { height, hash }
        };

        wallet_scan
            .update
            .insert_checkpoint(tip_at_start.height, tip_at_start.hash);

        for (keychain, spks) in keychains {
            let last_active_index = main_scan_loop(
                self,
                &mut wallet_scan.update,
                spks,
                stop_gap,
                parallel_requests,
            )?;

            if let Some(last_active_index) = last_active_index {
                wallet_scan
                    .last_active_indexes
                    .insert(keychain, last_active_index);
            }
        }

        let blocks_at_end = self
            .get_blocks(None)?
            .into_iter()
            .map(|block| BlockId {
                hash: block.id,
                height: block.time.height,
            })
            .collect::<Vec<_>>();

        if !blocks_at_end.contains(&tip_at_start) {
            return self.wallet_scan(backup_spks, stop_gap, existing_chain, parallel_requests);
        }

        for block in blocks_at_end {
            wallet_scan
                .update
                .insert_checkpoint(block.height, block.hash);
        }

        Ok(wallet_scan)
    }
}
