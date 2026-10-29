//! The decoder worker, the sweep, and the `events` debug command.

use crate::queue::{object_keys, Queue};
use crate::Common;
use anyhow::Result;
use async_trait::async_trait;
use celld_cdc_core::bucket::Bucket;
use celld_cdc_core::catalog::{scopes_for_key, Catalog};
use celld_cdc_core::cursor::CursorStore;
use celld_cdc_core::lane::{Decoder, EventSink, Outcome, Step};
use celld_cdc_core::ChangeEvent;
use clap::Args;
use futures_util::stream::{self, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

#[derive(Args)]
pub struct Opts {
    /// The SQS queue S3 sends `.ltx` and `.ltxb` notifications to.
    #[arg(long, env = "CDC_DOORBELL_QUEUE_URL")]
    queue_url: String,
    /// Lanes run at once by this worker.
    #[arg(long, env = "CDC_LANE_CONCURRENCY", default_value_t = 16)]
    concurrency: usize,
}

#[derive(Args)]
pub struct SweepOpts {
    /// Seconds between sweeps; 0 sweeps once and exits.
    #[arg(long, env = "CDC_SWEEP_INTERVAL_SECS", default_value_t = 300)]
    interval_secs: u64,
    #[arg(long, env = "CDC_LANE_CONCURRENCY", default_value_t = 16)]
    concurrency: usize,
}

/// How a message's lanes ended, worst first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Verdict {
    /// Every named lane is caught up, or handed to its new holder.
    Done,
    /// Another worker holds a lane and may have listed before this object
    /// landed: look again once it lets go.
    Busy,
    /// A lane stopped at a missing txid.
    Gap,
    /// A lane failed; let the visibility timeout redeliver.
    Failed,
}

pub async fn run(common: &Common, opts: Opts) -> Result<()> {
    let decoder = Arc::new(common.decoder()?);
    let queue = Queue::connect(&opts.queue_url).await?;
    info!(worker = %common.worker_id, "decoder started");
    loop {
        let messages = match queue.receive(10, 20).await {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "receive failed");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        // Many doorbells for one actor in a batch run its lane once.
        let mut lanes: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut verdicts = vec![Verdict::Done; messages.len()];
        for (i, m) in messages.iter().enumerate() {
            for key in object_keys(m.body().unwrap_or_default()) {
                match scopes_for_key(&decoder.catalog, &key).await {
                    Ok(scopes) => {
                        for scope in scopes {
                            lanes.entry(scope).or_default().push(i);
                        }
                    }
                    Err(e) => {
                        warn!(key, error = %e, "doorbell key");
                        verdicts[i] = Verdict::Failed;
                    }
                }
            }
        }
        let results: Vec<(Vec<usize>, Verdict)> = stream::iter(lanes)
            .map(|(scope, msgs)| {
                let decoder = decoder.clone();
                async move { (msgs, lane(&decoder, &scope).await) }
            })
            .buffer_unordered(opts.concurrency.max(1))
            .collect()
            .await;
        for (msgs, v) in results {
            for i in msgs {
                verdicts[i] = verdicts[i].max(v);
            }
        }
        for (m, v) in messages.iter().zip(verdicts) {
            let r = match v {
                Verdict::Done => queue.delete(m).await,
                Verdict::Busy => queue.retry_in(m, 2).await,
                Verdict::Gap => queue.retry_in(m, 5).await,
                Verdict::Failed => Ok(()),
            };
            if let Err(e) = r {
                warn!(error = %e, "settle message");
            }
        }
    }
}

async fn lane(decoder: &Decoder, scope: &str) -> Verdict {
    match decoder.run_lane(scope).await {
        Ok(Outcome::CaughtUp { pos, applied }) => {
            if applied > 0 {
                info!(scope, applied, ?pos, "lane caught up");
            }
            Verdict::Done
        }
        Ok(Outcome::LeaseLost { applied }) => {
            warn!(scope, applied, "lane lease lost");
            Verdict::Done
        }
        Ok(Outcome::Busy) => {
            debug!(scope, "lane busy");
            Verdict::Busy
        }
        Ok(Outcome::Gap { pos, applied, want }) => {
            info!(scope, applied, ?pos, want, "lane waiting for a missing txid");
            Verdict::Gap
        }
        Err(e) => {
            warn!(scope, error = format!("{e:#}"), "lane failed");
            Verdict::Failed
        }
    }
}

/// Every actor with objects in the bucket: `cells/` scopes plus scopes
/// seen only in live bundles so far.
async fn all_scopes(catalog: &Catalog) -> Result<Vec<String>> {
    let mut scopes: Vec<String> = catalog
        .celld
        .list_dirs("cells/")
        .await?
        .into_iter()
        .filter_map(|d| d.strip_prefix("cells/")?.strip_suffix('/').map(str::to_string))
        .collect();
    scopes.extend(catalog.bundle_segments(None).await?.into_iter().map(|(s, _)| s));
    scopes.sort();
    scopes.dedup();
    Ok(scopes)
}

pub async fn sweep(common: &Common, opts: SweepOpts) -> Result<()> {
    let decoder = Arc::new(common.decoder()?);
    loop {
        let scopes = all_scopes(&decoder.catalog).await?;
        let verdicts: Vec<Verdict> = stream::iter(scopes.iter())
            .map(|scope| {
                let decoder = decoder.clone();
                async move { lane(&decoder, scope).await }
            })
            .buffer_unordered(opts.concurrency.max(1))
            .collect()
            .await;
        let behind = verdicts.iter().filter(|v| **v != Verdict::Done).count();
        info!(actors = scopes.len(), behind, "sweep finished");
        if opts.interval_secs == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(opts.interval_secs)).await;
    }
}

struct Print;

#[async_trait]
impl EventSink for Print {
    async fn write(&self, _step: &Step, events: &[ChangeEvent]) -> Result<()> {
        for e in events {
            println!("{}", serde_json::to_string(e)?);
        }
        Ok(())
    }
}

/// Run one lane from scratch against a throwaway cursor and archive.
pub async fn print_events(common: &Common, scope: &str) -> Result<()> {
    let (celld, _) = common.buckets()?;
    let scratch = Bucket::memory();
    let decoder = Decoder {
        catalog: Arc::new(Catalog::new(celld, scratch.clone())),
        cursors: CursorStore::new(scratch, "events", common.lease_ms),
        sink: Arc::new(Print),
        mode: common.diff_mode()?,
    };
    let outcome = decoder.run_lane(scope).await?;
    eprintln!("{outcome:?}");
    Ok(())
}
