//! Materialize change events into SCD Type 1 and Type 2 Delta Lake tables.
//!
//! Two Delta tables live under the root: `tables/scd1` and `tables/scd2`,
//! each partitioned by `class` and `table_name`. There is one table per kind
//! rather than one per source table because the row data is generic JSON:
//! every source table has the same columns, a new SQLite table needs no DDL,
//! and the partitioning still lets readers prune to one table.
//!
//! Both tables are order-independent and idempotent: applying any set of
//! events in any batching, order, or with duplicates converges on the same
//! contents.
//!
//! - **SCD1** keeps, per `(scope, table_name, pk)`, the event with the
//!   greatest `seq`. It is one MERGE that updates only when
//!   `source.seq > target.seq`, so an older replay never wins. Deletes are
//!   soft (`is_deleted = true`, `row = null`): the tombstone keeps its seq, so
//!   a replayed older insert cannot resurrect the row.
//! - **SCD2** keeps one version per event. A delete is a tombstone version
//!   (`is_deleted = true`, `row = null`). For the keys a batch touches, the
//!   existing versions are read back, unioned with the incoming ones, deduped
//!   on `valid_from_seq` (equivalently `event_id`), sorted, and
//!   `valid_to_*`/`is_current` are recomputed as a LEAD. The changed and new
//!   versions go in as one MERGE keyed by
//!   `(class, table_name, scope, pk, valid_from_seq)`: matched rows are
//!   updated, unmatched ones inserted, in a single Delta commit. Versions are
//!   never removed, so MERGE suffices; delta-rs has no atomic delete+append.
//!
//! When two copies of one `event_id` disagree in content (they should not:
//! ids come from the segment position), the incoming duplicates are reduced
//! deterministically, but the copy already in the table wins.
//!
//! SCD1 and SCD2 are separate Delta tables and so separate commits. A failed
//! `apply` can leave one ahead of the other; retrying the same events fixes
//! that, because both are idempotent. Concurrent writers to one table are
//! made safe by Delta's optimistic commits (S3 conditional put); a conflict
//! surfaces as an error and the caller retries.

mod rows;

pub use rows::{RowKey, Scd1Row, Scd2Row, PARTITION_COLUMNS};

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{anyhow, Context, Result};
use celld_cdc_core::{ChangeEvent, Op};
use deltalake::arrow::array::RecordBatch;
use deltalake::datafusion::prelude::{col, lit, Expr, SessionContext};
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use rows::{Col, SCD1_COLS, SCD1_KEY, SCD2_COLS, SCD2_KEY};

/// What one [`Materializer::apply`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ApplyStats {
    /// Events passed in.
    pub events: usize,
    /// Events left after deduping on key and seq.
    pub unique_events: usize,
    /// Distinct `(class, table_name, scope, pk)` keys touched.
    pub keys: usize,
    pub scd1_inserted: usize,
    pub scd1_updated: usize,
    pub scd2_inserted: usize,
    pub scd2_updated: usize,
}

/// Writes change events into the SCD1 and SCD2 Delta tables.
pub struct Materializer {
    scd1: DeltaTable,
    scd2: DeltaTable,
}

/// Storage options for S3-compatible stores such as LocalStack or MinIO:
/// a custom endpoint, plain HTTP, path-style addressing, and ETag
/// conditional put (object_store's default, set explicitly) so Delta
/// commits are safe without a DynamoDB lock.
pub fn s3_compatible_storage_options(
    endpoint: &str,
    region: &str,
    access_key_id: &str,
    secret_access_key: &str,
) -> HashMap<String, String> {
    [
        ("AWS_ENDPOINT_URL", endpoint),
        ("AWS_REGION", region),
        ("AWS_ACCESS_KEY_ID", access_key_id),
        ("AWS_SECRET_ACCESS_KEY", secret_access_key),
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
        ("AWS_CONDITIONAL_PUT", "etag"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

impl Materializer {
    /// Opens (creating if missing) `{root}/tables/scd1` and
    /// `{root}/tables/scd2`. `root` is a URL (`s3://bucket/prefix`,
    /// `file:///…`) or a local path.
    pub async fn open(
        table_root_url: &str,
        storage_options: HashMap<String, String>,
    ) -> Result<Self> {
        deltalake::aws::register_handlers(None);
        let root = table_root_url.trim_end_matches('/');
        let scd1 = open_or_create(&format!("{root}/tables/scd1"), &storage_options, SCD1_COLS).await?;
        let scd2 = open_or_create(&format!("{root}/tables/scd2"), &storage_options, SCD2_COLS).await?;
        Ok(Self { scd1, scd2 })
    }

    /// The SCD1 table, as of the last apply or read.
    pub fn scd1(&self) -> &DeltaTable {
        &self.scd1
    }

    /// The SCD2 table, as of the last apply or read.
    pub fn scd2(&self) -> &DeltaTable {
        &self.scd2
    }

    /// Applies a batch of events to both tables: SCD1 first, then SCD2.
    pub async fn apply(&mut self, events: Vec<ChangeEvent>) -> Result<ApplyStats> {
        let mut stats = ApplyStats { events: events.len(), ..Default::default() };
        // key -> valid_from_seq -> version. One seq per key is one event.
        let mut incoming: BTreeMap<RowKey, BTreeMap<String, Scd2Row>> = BTreeMap::new();
        for e in &events {
            let v = version_of(e)?;
            keep_min(incoming.entry(v.key()).or_default(), v);
        }
        stats.keys = incoming.len();
        stats.unique_events = incoming.values().map(BTreeMap::len).sum();
        if incoming.is_empty() {
            return Ok(stats);
        }
        (stats.scd1_inserted, stats.scd1_updated) = self.apply_scd1(&incoming).await?;
        (stats.scd2_inserted, stats.scd2_updated) = self.apply_scd2(incoming).await?;
        Ok(stats)
    }

    /// Decodes change-log Parquet files (see
    /// [`celld_cdc_parquet::write_events`]) and applies all their events in
    /// one [`apply`](Self::apply).
    pub async fn apply_change_files(&mut self, files: &[bytes::Bytes]) -> Result<ApplyStats> {
        let mut events = Vec::new();
        for (i, f) in files.iter().enumerate() {
            events.extend(
                celld_cdc_parquet::read_events(f)
                    .with_context(|| format!("decoding change file {i}"))?,
            );
        }
        self.apply(events).await
    }

    async fn apply_scd1(
        &mut self,
        incoming: &BTreeMap<RowKey, BTreeMap<String, Scd2Row>>,
    ) -> Result<(usize, usize)> {
        let rows: Vec<Scd1Row> = incoming
            .values()
            .filter_map(|versions| versions.values().next_back())
            .map(scd1_of)
            .collect();
        self.scd1.update_state().await?;
        let batch = rows::scd1_batch(&rows)?;
        let (table, m) = merge(self.scd1.clone(), batch, SCD1_COLS, SCD1_KEY, Some("seq")).await?;
        self.scd1 = table;
        Ok(m)
    }

    async fn apply_scd2(
        &mut self,
        mut incoming: BTreeMap<RowKey, BTreeMap<String, Scd2Row>>,
    ) -> Result<(usize, usize)> {
        self.scd2.update_state().await?;
        let keys: BTreeSet<RowKey> = incoming.keys().cloned().collect();
        let existing = self.read_scd2(Some(&keys)).await?;

        let mut stored: HashMap<(RowKey, String), Scd2Row> = HashMap::new();
        for v in existing {
            if !keys.contains(&v.key()) {
                continue;
            }
            let versions = incoming.get_mut(&v.key()).expect("key is affected");
            // The stored copy of a version wins over an incoming duplicate.
            let mut candidate = v.clone();
            candidate.valid_to_seq = None;
            candidate.valid_to_ts_ms = None;
            candidate.is_current = false;
            versions.insert(candidate.valid_from_seq.clone(), candidate);
            stored.insert((v.key(), v.valid_from_seq.clone()), v);
        }

        let mut changed = Vec::new();
        for (key, versions) in incoming {
            let mut versions: Vec<Scd2Row> = versions.into_values().collect();
            let next: Vec<(Option<String>, Option<i64>)> = versions
                .iter()
                .skip(1)
                .map(|n| (Some(n.valid_from_seq.clone()), Some(n.valid_from_ts_ms)))
                .chain([(None, None)])
                .collect();
            for (v, (to_seq, to_ts)) in versions.iter_mut().zip(next) {
                v.is_current = to_seq.is_none();
                v.valid_to_seq = to_seq;
                v.valid_to_ts_ms = to_ts;
            }
            for v in versions {
                if stored.get(&(key.clone(), v.valid_from_seq.clone())) != Some(&v) {
                    changed.push(v);
                }
            }
        }
        if changed.is_empty() {
            return Ok((0, 0));
        }
        let batch = rows::scd2_batch(&changed)?;
        let (table, m) = merge(self.scd2.clone(), batch, SCD2_COLS, SCD2_KEY, None).await?;
        self.scd2 = table;
        Ok(m)
    }

    /// Every SCD1 row at the latest table version, sorted.
    pub async fn read_scd1(&mut self) -> Result<Vec<Scd1Row>> {
        self.scd1.update_state().await?;
        let mut rows = Vec::new();
        for b in scan(&self.scd1, None).await? {
            rows.extend(rows::scd1_from_batch(&b)?);
        }
        rows.sort();
        Ok(rows)
    }

    /// SCD2 versions, sorted: all of them, or only those of `keys`.
    pub async fn read_scd2(&mut self, keys: Option<&BTreeSet<RowKey>>) -> Result<Vec<Scd2Row>> {
        self.scd2.update_state().await?;
        let filter = keys.map(key_filter);
        let mut rows = Vec::new();
        for b in scan(&self.scd2, filter).await? {
            rows.extend(
                rows::scd2_from_batch(&b)?
                    .into_iter()
                    .filter(|r| keys.is_none_or(|k| k.contains(&r.key()))),
            );
        }
        rows.sort();
        Ok(rows)
    }
}

/// Keeps the smaller of two versions at one seq, so duplicates reduce the
/// same way whatever order they arrive in.
fn keep_min(versions: &mut BTreeMap<String, Scd2Row>, v: Scd2Row) {
    match versions.get(&v.valid_from_seq) {
        Some(old) if *old <= v => {}
        _ => {
            versions.insert(v.valid_from_seq.clone(), v);
        }
    }
}

fn to_i64(v: u64, what: &str) -> Result<i64> {
    i64::try_from(v).map_err(|_| anyhow!("{what} {v} exceeds i64::MAX"))
}

/// An event as an SCD2 version, before `valid_to`/`is_current` are known.
fn version_of(e: &ChangeEvent) -> Result<Scd2Row> {
    let is_deleted = e.op == Op::Delete;
    Ok(Scd2Row {
        scope: e.scope.clone(),
        class: e.class.clone(),
        actor_name: e.actor_name.clone(),
        table_name: e.table.clone(),
        pk: e.pk.to_string(),
        row: if is_deleted { None } else { e.after.as_ref().map(|v| v.to_string()) },
        op: e.op.as_str().to_string(),
        is_deleted,
        epoch: to_i64(e.epoch, "epoch")?,
        txid: to_i64(e.txid, "txid")?,
        valid_from_seq: e.seq.clone(),
        valid_to_seq: None,
        valid_from_ts_ms: e.commit_ts_ms,
        valid_to_ts_ms: None,
        is_current: false,
        event_id: e.event_id.clone(),
    })
}

fn scd1_of(v: &Scd2Row) -> Scd1Row {
    Scd1Row {
        scope: v.scope.clone(),
        class: v.class.clone(),
        actor_name: v.actor_name.clone(),
        table_name: v.table_name.clone(),
        pk: v.pk.clone(),
        row: v.row.clone(),
        is_deleted: v.is_deleted,
        op: v.op.clone(),
        epoch: v.epoch,
        txid: v.txid,
        seq: v.valid_from_seq.clone(),
        commit_ts_ms: v.valid_from_ts_ms,
        event_id: v.event_id.clone(),
    }
}

async fn open_or_create(
    uri: &str,
    storage_options: &HashMap<String, String>,
    cols: &[Col],
) -> Result<DeltaTable> {
    let url = deltalake::ensure_table_uri(uri).with_context(|| format!("bad table URI {uri}"))?;
    let load = || DeltaTable::try_from_url_with_storage_options(url.clone(), storage_options.clone());
    let table = load().await.with_context(|| format!("opening {url}"))?;
    if table.version().is_some() {
        return Ok(table);
    }
    let created = table
        .create()
        .with_columns(rows::delta_fields(cols))
        .with_partition_columns(PARTITION_COLUMNS)
        .with_save_mode(SaveMode::Ignore)
        .await;
    match created {
        Ok(t) => Ok(t),
        // A concurrent materializer may have created it first.
        Err(err) => match load().await {
            Ok(t) if t.version().is_some() => Ok(t),
            _ => Err(err).with_context(|| format!("creating {url}")),
        },
    }
}

/// `(class, table_name)` partition pruning plus `scope`/`pk` IN lists; the
/// caller filters exact keys.
fn key_filter(keys: &BTreeSet<RowKey>) -> Expr {
    let parts: BTreeSet<(&str, &str)> = keys.iter().map(|k| (k.0.as_str(), k.1.as_str())).collect();
    let part = parts
        .into_iter()
        .map(|(c, t)| col("class").eq(lit(c)).and(col("table_name").eq(lit(t))))
        .reduce(Expr::or)
        .expect("keys is not empty");
    let in_list = |name: &str, vals: BTreeSet<&str>| {
        col(name).in_list(vals.into_iter().map(lit).collect(), false)
    };
    part.and(in_list("scope", keys.iter().map(|k| k.2.as_str()).collect()))
        .and(in_list("pk", keys.iter().map(|k| k.3.as_str()).collect()))
}

async fn scan(table: &DeltaTable, filter: Option<Expr>) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state())?;
    let mut df = ctx.read_table(table.table_provider().await?)?;
    if let Some(f) = filter {
        df = df.filter(f)?;
    }
    Ok(df.collect().await?)
}

/// MERGE `batch` into `table` on `key`: update every other column when
/// matched (and, with `newer`, only when the source's value of that column
/// is greater), insert when not. Returns `(inserted, updated)`.
async fn merge(
    table: DeltaTable,
    batch: RecordBatch,
    cols: &[Col],
    key: &[&str],
    newer: Option<&str>,
) -> Result<(DeltaTable, (usize, usize))> {
    let src = |n: &str| col(format!("source.{n}"));
    let predicate = key
        .iter()
        .map(|k| col(format!("target.{k}")).eq(src(k)))
        .reduce(Expr::and)
        .expect("key is not empty");
    let source = SessionContext::new().read_batch(batch)?;
    let (table, metrics) = table
        .merge(source, predicate)
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|mut u| {
            if let Some(c) = newer {
                u = u.predicate(src(c).gt(col(format!("target.{c}"))));
            }
            cols.iter()
                .filter(|(n, _, _)| !key.contains(n))
                .fold(u, |u, (n, _, _)| u.update(*n, src(n)))
        })?
        .when_not_matched_insert(|i| cols.iter().fold(i, |i, (n, _, _)| i.set(*n, src(n))))?
        .await?;
    Ok((table, (metrics.num_target_rows_inserted, metrics.num_target_rows_updated)))
}
