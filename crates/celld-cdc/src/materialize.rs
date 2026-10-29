//! The materializer worker: change files in, SCD1/SCD2 Delta commits out.
//!
//! Applying change files is idempotent and order-independent, so a batch
//! that fails is simply redelivered, and the periodic rescan of recent
//! partitions repairs any change file whose notification was lost.

use crate::queue::{object_keys, Queue};
use crate::Common;
use anyhow::{Context, Result};
use celld_cdc_core::bucket::Bucket;
use celld_cdc_core::cursor::now_ms;
use celld_cdc_materialize::{s3_compatible_storage_options, Materializer};
use clap::Args;
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};
use tracing::{info, warn};

#[derive(Args)]
pub struct Opts {
    /// The SQS queue S3 sends `changes/*.parquet` notifications to.
    #[arg(long, env = "CDC_CHANGES_QUEUE_URL")]
    queue_url: String,
    /// Longest wait to fill one batch, and so one Delta commit.
    #[arg(long, env = "CDC_BATCH_SECS", default_value_t = 5)]
    batch_secs: u64,
    #[arg(long, env = "CDC_BATCH_MAX_FILES", default_value_t = 500)]
    batch_max_files: usize,
    /// Seconds between rescans of recent change-log partitions; 0 disables.
    #[arg(long, env = "CDC_RESCAN_SECS", default_value_t = 300)]
    rescan_secs: u64,
    /// How many recent hours a rescan covers.
    #[arg(long, env = "CDC_RESCAN_HOURS", default_value_t = 2)]
    rescan_hours: i64,
}

fn storage_options(common: &Common) -> HashMap<String, String> {
    let s3 = common.s3();
    if common.endpoint.is_empty() {
        HashMap::from([("AWS_REGION".to_string(), s3.region)])
    } else {
        s3_compatible_storage_options(&s3.endpoint, &s3.region, &s3.access_key_id, &s3.secret_access_key)
    }
}

pub async fn run(common: &Common, opts: Opts) -> Result<()> {
    let (_, cdc) = common.buckets()?;
    let root = common.cdc_bucket.trim_end_matches('/').to_string();
    let mut mat = Materializer::open(&root, storage_options(common))
        .await
        .with_context(|| format!("open tables under {root}"))?;
    let queue = Queue::connect(&opts.queue_url).await?;
    info!(root, "materializer started");
    let mut last_rescan = Instant::now();
    loop {
        let deadline = Instant::now() + Duration::from_secs(opts.batch_secs);
        let mut messages = Vec::new();
        let mut keys = BTreeSet::new();
        while Instant::now() < deadline && keys.len() < opts.batch_max_files {
            let wait = deadline.saturating_duration_since(Instant::now()).as_secs().clamp(1, 20) as i32;
            match queue.receive(10, wait).await {
                Ok(batch) => {
                    for m in batch {
                        keys.extend(
                            object_keys(m.body().unwrap_or_default())
                                .into_iter()
                                .filter(|k| is_change_file(&cdc, k)),
                        );
                        messages.push(m);
                    }
                }
                Err(e) => {
                    warn!(error = %e, "receive failed");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        if !keys.is_empty() {
            match apply(&mut mat, &cdc, &keys).await {
                Ok(()) => {
                    for m in &messages {
                        if let Err(e) = queue.delete(m).await {
                            warn!(error = %e, "delete message");
                        }
                    }
                }
                // Leave the messages; they come back after the visibility
                // timeout and the apply is retried whole.
                Err(e) => warn!(error = format!("{e:#}"), files = keys.len(), "apply failed"),
            }
        } else {
            for m in &messages {
                let _ = queue.delete(m).await;
            }
        }
        if opts.rescan_secs > 0 && last_rescan.elapsed() >= Duration::from_secs(opts.rescan_secs) {
            last_rescan = Instant::now();
            if let Err(e) = rescan(&mut mat, &cdc, opts.rescan_hours).await {
                warn!(error = format!("{e:#}"), "rescan failed");
            }
        }
    }
}

fn is_change_file(cdc: &Bucket, key: &str) -> bool {
    let rel = key.strip_prefix(cdc.prefix()).unwrap_or(key);
    rel.starts_with("changes/") && rel.ends_with(".parquet")
}

async fn apply(mat: &mut Materializer, cdc: &Bucket, keys: &BTreeSet<String>) -> Result<()> {
    let mut events = Vec::new();
    for key in keys {
        let rel = key.strip_prefix(cdc.prefix()).unwrap_or(key);
        let Some(obj) = cdc.get(rel).await? else {
            // Deleted by compaction after an earlier apply; nothing to do.
            continue;
        };
        events.extend(celld_cdc_parquet::read_events(&obj.bytes).with_context(|| format!("read {rel}"))?);
    }
    let stats = mat.apply(events).await?;
    info!(files = keys.len(), ?stats, "applied change files");
    Ok(())
}

/// Re-apply every change file of the last `hours` hours.
async fn rescan(mat: &mut Materializer, cdc: &Bucket, hours: i64) -> Result<()> {
    let now = now_ms();
    let mut keys = BTreeSet::new();
    for class_dir in cdc.list_dirs("changes/").await? {
        for h in 0..hours.max(1) {
            let (dt, hh) = celld_cdc_parquet::dt_hh(now - h * 3_600_000);
            for (key, _) in cdc.list(&format!("{class_dir}dt={dt}/hh={hh}/")).await? {
                if key.ends_with(".parquet") {
                    keys.insert(key);
                }
            }
        }
    }
    if !keys.is_empty() {
        apply(mat, cdc, &keys).await?;
    }
    Ok(())
}

/// Print a table as JSON lines: `scd1`, `scd2`, or `changes` (every event
/// in the change log).
pub async fn dump(common: &Common, table: &str) -> Result<()> {
    let (_, cdc) = common.buckets()?;
    if table == "changes" {
        for (key, _) in cdc.list("changes/").await? {
            for e in celld_cdc_parquet::read_events(&cdc.get_bytes(&key).await?)? {
                println!("{}", serde_json::to_string(&e)?);
            }
        }
        return Ok(());
    }
    let root = common.cdc_bucket.trim_end_matches('/').to_string();
    let mut mat = Materializer::open(&root, storage_options(common)).await?;
    match table {
        "scd1" => {
            for r in mat.read_scd1().await? {
                println!("{}", serde_json::to_string(&r)?);
            }
        }
        "scd2" => {
            for r in mat.read_scd2(None).await? {
                println!("{}", serde_json::to_string(&r)?);
            }
        }
        other => anyhow::bail!("unknown table {other}; use scd1, scd2, or changes"),
    }
    Ok(())
}
