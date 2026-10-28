//! `celld-cdc`: stateless workers that turn celld's bucket into change logs
//! and SCD tables.
//!
//! - `decode` turns doorbells (S3 notifications for `.ltx` and `.ltxb`
//!   objects) into Parquet change files, one ordered lane per actor;
//! - `materialize` folds change files into the SCD1 and SCD2 Delta tables;
//! - `sweep` catches up every actor whose doorbell was lost;
//! - `events` prints one actor's history as JSON lines, touching nothing;
//! - `dump` prints a table or the change log as JSON lines.

mod decode;
mod materialize;
mod queue;
mod sink;

use anyhow::Result;
use celld_cdc_core::bucket::{Bucket, S3Config};
use celld_cdc_core::catalog::Catalog;
use celld_cdc_core::cursor::CursorStore;
use celld_cdc_core::lane::{Decoder, DiffMode};
use clap::{Args, Parser, Subcommand};
use std::sync::Arc;

#[derive(Parser)]
#[command(version, about = "Change data capture for celld")]
struct Cli {
    #[command(flatten)]
    common: Common,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
pub struct Common {
    /// celld's fleet bucket, read only.
    #[arg(long, env = "CELLD_BUCKET", default_value = "s3://celld", global = true)]
    celld_bucket: String,
    /// celld-cdc's own bucket: cursors, archived segments, change logs, tables.
    #[arg(long, env = "CDC_BUCKET", default_value = "s3://celld-cdc", global = true)]
    cdc_bucket: String,
    /// S3-compatible endpoint, e.g. http://localstack:4566. Empty for AWS.
    #[arg(long, env = "S3_ENDPOINT", default_value = "", global = true)]
    endpoint: String,
    #[arg(long, env = "AWS_REGION", default_value = "us-east-1", global = true)]
    region: String,
    /// This worker's lease-holder name.
    #[arg(long, env = "CDC_WORKER_ID", default_value_t = default_worker_id(), global = true)]
    worker_id: String,
    /// How lanes find row changes: `paged` reads only the pages a segment
    /// changed; `full` downloads and compares whole databases.
    #[arg(long, env = "CDC_DIFF", default_value = "paged", global = true)]
    diff: String,
    /// How long a lane lease lasts without renewal.
    #[arg(long, env = "CDC_LEASE_MS", default_value_t = 60_000, global = true)]
    lease_ms: i64,
}

#[derive(Subcommand)]
enum Cmd {
    /// Consume doorbells and write change files.
    Decode(decode::Opts),
    /// Consume change-file notifications and update the SCD tables.
    Materialize(materialize::Opts),
    /// Run every actor's lane, on an interval or once.
    Sweep(decode::SweepOpts),
    /// Print `scd1`, `scd2`, or `changes` as JSON lines.
    Dump {
        table: String,
    },
    /// Print one actor's events from its first segment, as JSON lines.
    Events {
        /// The cell scope, `Class:hex`.
        scope: String,
    },
}

fn default_worker_id() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| format!("worker-{}", std::process::id()))
}

impl Common {
    fn s3(&self) -> S3Config {
        S3Config {
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
        }
    }

    fn buckets(&self) -> Result<(Bucket, Bucket)> {
        let s3 = self.s3();
        Ok((Bucket::open(&self.celld_bucket, &s3)?, Bucket::open(&self.cdc_bucket, &s3)?))
    }

    fn decoder(&self) -> Result<Decoder> {
        let (celld, cdc) = self.buckets()?;
        Ok(Decoder {
            catalog: Arc::new(Catalog::new(celld, cdc.clone())),
            cursors: CursorStore::new(cdc.clone(), self.worker_id.clone(), self.lease_ms),
            sink: Arc::new(sink::ParquetSink { cdc }),
            mode: self.diff_mode()?,
        })
    }

    fn diff_mode(&self) -> Result<DiffMode> {
        match self.diff.as_str() {
            "paged" => Ok(DiffMode::Paged),
            "full" => Ok(DiffMode::Full),
            other => anyhow::bail!("unknown --diff {other}; use paged or full"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,aws_config=warn,deltalake=warn,datafusion=warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Decode(opts) => decode::run(&cli.common, opts).await,
        Cmd::Materialize(opts) => materialize::run(&cli.common, opts).await,
        Cmd::Sweep(opts) => decode::sweep(&cli.common, opts).await,
        Cmd::Dump { table } => materialize::dump(&cli.common, &table).await,
        Cmd::Events { scope } => decode::print_events(&cli.common, &scope).await,
    }
}
