//! List an actor's segments from the buckets and fetch their bytes.

use crate::bucket::Bucket;
use crate::key::{self, BundleKey, CelldKey};
use crate::segment::{Segment, Source};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use celld_ltx::bundle::BundleRow;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Bytes of ranged reads kept in memory. Every object read is immutable, so
/// the cache is never stale; it saves the page indexes and frames a lane
/// rereads on every run.
const RANGE_CACHE_BYTES: usize = 128 << 20;

#[derive(Default)]
struct RangeCache {
    map: HashMap<(String, u64, u64), Bytes>,
    order: VecDeque<(String, u64, u64)>,
    bytes: usize,
}

impl RangeCache {
    fn get(&self, k: &(String, u64, u64)) -> Option<Bytes> {
        self.map.get(k).cloned()
    }

    fn put(&mut self, k: (String, u64, u64), v: Bytes) {
        if v.len() > RANGE_CACHE_BYTES / 16 || self.map.contains_key(&k) {
            return;
        }
        self.bytes += v.len();
        self.order.push_back(k.clone());
        self.map.insert(k, v);
        while self.bytes > RANGE_CACHE_BYTES {
            let Some(old) = self.order.pop_front() else { break };
            if let Some(b) = self.map.remove(&old) {
                self.bytes -= b.len();
            }
        }
    }
}

/// The two buckets: celld's fleet bucket (read only) and celld-cdc's own.
pub struct Catalog {
    pub celld: Bucket,
    pub cdc: Bucket,
    /// Bundle footers by key. A bundle never changes once written, so this
    /// is a pure cache; entries for bundles no longer listed are pruned.
    footers: Mutex<HashMap<String, Vec<BundleRow>>>,
    ranges: Mutex<RangeCache>,
    /// Segment bytes read from the buckets (cache misses), for operators.
    fetched: std::sync::atomic::AtomicU64,
}

/// `segments/{scope}/e{epoch}/{min:016x}-{max:016x}.ltx` in the cdc bucket.
pub fn archive_key(scope: &str, epoch: u64, min: u64, max: u64) -> String {
    format!("segments/{scope}/e{epoch}/{min:016x}-{max:016x}.ltx")
}

impl Catalog {
    pub fn new(celld: Bucket, cdc: Bucket) -> Self {
        Self {
            celld,
            cdc,
            footers: Mutex::new(HashMap::new()),
            ranges: Mutex::new(RangeCache::default()),
            fetched: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Every segment known for `scope`.
    pub async fn segments(&self, scope: &str) -> Result<Vec<Segment>> {
        let (cells, archived, bundled) = tokio::try_join!(
            self.cell_segments(scope),
            self.archived_segments(scope),
            self.bundle_segments(Some(scope)),
        )?;
        let mut all = cells;
        all.extend(archived);
        all.extend(bundled.into_iter().map(|(_, s)| s));
        Ok(all)
    }

    async fn cell_segments(&self, scope: &str) -> Result<Vec<Segment>> {
        let prefix = self.celld.prefix().to_string();
        let listed = self.celld.list(&key::ltx_root(scope)).await?;
        Ok(listed
            .into_iter()
            .filter_map(|(k, meta)| match key::parse("", &k)? {
                CelldKey::Ltx(l) if l.scope == scope => Some(Segment {
                    epoch: l.epoch,
                    level: l.level,
                    min: l.min_txid,
                    max: l.max_txid,
                    size: meta.size,
                    source: Source::Cell {
                        key: format!("{prefix}{k}"),
                    },
                }),
                _ => None,
            })
            .collect())
    }

    async fn archived_segments(&self, scope: &str) -> Result<Vec<Segment>> {
        let listed = self.cdc.list(&format!("segments/{scope}/")).await?;
        Ok(listed
            .into_iter()
            .filter_map(|(k, meta)| {
                let rest = k.strip_prefix(&format!("segments/{scope}/e"))?;
                let (epoch, name) = rest.split_once('/')?;
                let (min, max) = name.strip_suffix(".ltx")?.split_once('-')?;
                Some(Segment {
                    epoch: epoch.parse().ok()?,
                    level: 0,
                    min: u64::from_str_radix(min, 16).ok()?,
                    max: u64::from_str_radix(max, 16).ok()?,
                    size: meta.size,
                    source: Source::Archive { key: k.clone() },
                })
            })
            .collect())
    }

    /// Segments in live bundles, for one scope or (with `None`) all, keyed
    /// by scope. A drained bundle is deleted, so this list stays short.
    pub async fn bundle_segments(&self, scope: Option<&str>) -> Result<Vec<(String, Segment)>> {
        let listed = self.celld.list("log/").await?;
        let mut live = Vec::new();
        let mut out = Vec::new();
        for (k, meta) in listed {
            let Some(CelldKey::Bundle(_)) = key::parse("", &k) else {
                continue;
            };
            live.push(k.clone());
            let rows = match self.footer(&k, meta.size).await {
                Ok(rows) => rows,
                // Drained and deleted between the list and the read.
                Err(e) if is_not_found(&e) => continue,
                Err(e) => return Err(e),
            };
            for row in rows {
                if scope.is_some_and(|s| s != row.cell) {
                    continue;
                }
                out.push((
                    row.cell.clone(),
                    Segment {
                        epoch: row.cell_epoch,
                        level: 0,
                        min: row.txid,
                        max: row.txid,
                        size: row.len,
                        source: Source::Bundle {
                            key: format!("{}{k}", self.celld.prefix()),
                            offset: row.offset,
                            len: row.len,
                        },
                    },
                ));
            }
        }
        self.footers
            .lock()
            .unwrap()
            .retain(|k, _| live.iter().any(|l| l == k));
        Ok(out)
    }

    /// The rows of one bundle, read with two ranged GETs: the 8-byte
    /// trailer (footer length and magic), then the footer.
    pub async fn footer(&self, key: &str, size: u64) -> Result<Vec<BundleRow>> {
        if let Some(rows) = self.footers.lock().unwrap().get(key) {
            return Ok(rows.clone());
        }
        if size < 8 {
            bail!("bundle {key} is shorter than its trailer");
        }
        let trailer = self.celld.get_range(key, size - 8..size).await?;
        let footer_len = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as u64;
        if footer_len + 8 > size {
            bail!("bundle {key} footer overruns the object");
        }
        let footer = self
            .celld
            .get_range(key, size - 8 - footer_len..size - 8)
            .await?;
        // `decode_rows` takes a whole bundle; hand it the footer with an
        // empty data section and the original trailer.
        let mut shell = footer.to_vec();
        shell.extend_from_slice(&trailer);
        let rows = celld_ltx::bundle::decode_rows(&shell).context("decode bundle footer")?;
        self.footers
            .lock()
            .unwrap()
            .insert(key.to_string(), rows.clone());
        Ok(rows)
    }

    /// Segment bytes read from the buckets so far.
    pub fn fetched_bytes(&self) -> u64 {
        self.fetched.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn count(&self, bytes: &Bytes) {
        self.fetched
            .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// The LTX bytes of a segment.
    pub async fn fetch(&self, seg: &Segment) -> Result<Bytes> {
        let bytes = self.fetch_uncounted(seg).await?;
        self.count(&bytes);
        Ok(bytes)
    }

    async fn fetch_uncounted(&self, seg: &Segment) -> Result<Bytes> {
        let strip = |k: &str| k.strip_prefix(self.celld.prefix()).unwrap_or(k).to_string();
        match &seg.source {
            Source::Cell { key } => self.celld.get_bytes(&strip(key)).await,
            Source::Bundle { key, offset, len } => {
                // Row offsets are into the data section, which starts the
                // object.
                self.celld
                    .get_range(&strip(key), *offset..offset + len)
                    .await
            }
            Source::Archive { key } => self.cdc.get_bytes(key).await,
        }
    }

    /// `len` bytes of a segment's LTX file from `offset`, cached.
    pub async fn fetch_range(&self, seg: &Segment, offset: u64, len: u64) -> Result<Bytes> {
        let k = (seg.source_key().to_string(), offset, len);
        if let Some(b) = self.ranges.lock().unwrap().get(&k) {
            return Ok(b);
        }
        let strip = |k: &str| k.strip_prefix(self.celld.prefix()).unwrap_or(k).to_string();
        let end = offset + len;
        let bytes = match &seg.source {
            Source::Cell { key } => self.celld.get_range(&strip(key), offset..end).await?,
            Source::Bundle { key, offset: base, .. } => {
                self.celld.get_range(&strip(key), base + offset..base + end).await?
            }
            Source::Archive { key } => self.cdc.get_range(key, offset..end).await?,
        };
        self.count(&bytes);
        self.ranges.lock().unwrap().put(k, bytes.clone());
        Ok(bytes)
    }

    /// Keep a bundle segment's bytes, so this state stays rebuildable after
    /// celld drains and deletes the bundle.
    pub async fn archive(&self, scope: &str, seg: &Segment, bytes: &Bytes) -> Result<()> {
        if matches!(seg.source, Source::Bundle { .. }) {
            let key = archive_key(scope, seg.epoch, seg.min, seg.max);
            self.cdc.put(&key, bytes.clone()).await?;
        }
        Ok(())
    }
}

fn is_not_found(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        matches!(
            c.downcast_ref::<object_store::Error>(),
            Some(object_store::Error::NotFound { .. })
        )
    })
}

/// Which actors a doorbell key names.
pub async fn scopes_for_key(catalog: &Catalog, key: &str) -> Result<Vec<String>> {
    match key::parse(catalog.celld.prefix(), key) {
        Some(CelldKey::Ltx(l)) => Ok(vec![l.scope]),
        Some(CelldKey::Bundle(b)) => bundle_scopes(catalog, &b).await,
        None => Ok(Vec::new()),
    }
}

async fn bundle_scopes(catalog: &Catalog, b: &BundleKey) -> Result<Vec<String>> {
    let rel = b.to_key("");
    // Listing is by directory, so list the bundle's own and find it there.
    let dir = rel.rsplit_once('/').map_or("", |(d, _)| d);
    let listed = catalog.celld.list(dir).await?;
    let Some((_, meta)) = listed.into_iter().find(|(k, _)| *k == rel) else {
        // Already drained: its segments are in `cells/` now.
        return Ok(Vec::new());
    };
    let mut scopes: Vec<String> = catalog
        .footer(&rel, meta.size)
        .await?
        .into_iter()
        .map(|r| r.cell)
        .collect();
    scopes.sort();
    scopes.dedup();
    Ok(scopes)
}
