//! The decoder's sink: one Parquet change file per segment.

use anyhow::Result;
use async_trait::async_trait;
use celld_cdc_core::bucket::Bucket;
use celld_cdc_core::event::class_of;
use celld_cdc_core::lane::{EventSink, Step};
use celld_cdc_core::ChangeEvent;

pub struct ParquetSink {
    pub cdc: Bucket,
}

#[async_trait]
impl EventSink for ParquetSink {
    /// A segment with no captured changes (only celld's own bookkeeping)
    /// writes nothing. The key and bytes are both functions of the segment,
    /// so a rewrite after a lost lease is the same object.
    async fn write(&self, step: &Step, events: &[ChangeEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let key = celld_cdc_parquet::change_log_key(
            "",
            class_of(&step.scope),
            step.commit_ts_ms,
            &step.scope,
            step.epoch,
            step.min_txid,
            step.max_txid,
        );
        self.cdc.put(&key, celld_cdc_parquet::write_events(events)?).await
    }
}
