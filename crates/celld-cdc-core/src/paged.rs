//! An actor's database at one position, paged in from the bucket on demand.
//!
//! A [`PagedDb`] is a page map over a plan of segments (celld-ltx's
//! `build_page_map`: each segment's trailing page index, never its frames)
//! behind celld-ltx's paged VFS: SQLite opens a sparse local file, and each
//! page it reads is fetched with one ranged GET the first time. Opening a
//! large actor therefore costs its page indexes, and a query costs the pages
//! it touches. An `overlay` segment (the one being diffed) is served from
//! memory on top of the plan, so the "after" side needs no copy of "before".
//!
//! A fault fetches exactly its page by default: a diff reads scattered
//! pages, and reading ahead would drag in their neighbours. [`PagedDb::scan`]
//! switches to celld-ltx's read-ahead runs for a whole-table comparison.

use crate::catalog::Catalog;
use crate::segment::Segment;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use celld_ltx::client::ReplicaClient;
use celld_ltx::ltx::FileInfo;
use celld_ltx::paged::{build_page_map, PageMap, RangeReader, READAHEAD_BYTES};
use celld_ltx::paged_vfs::{self, PageReader};
use celld_ltx::TXID;
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Handle;

/// The segments one page map reads, by their `(level, min, max)`.
struct Files {
    catalog: Arc<Catalog>,
    by_range: HashMap<(i32, u64, u64), Segment>,
    overlay: Option<(Segment, Bytes)>,
    handle: Handle,
}

fn info(seg: &Segment) -> FileInfo {
    FileInfo {
        level: seg.level as i32,
        min_txid: TXID(seg.min),
        max_txid: TXID(seg.max),
        size: seg.size as i64,
        ..Default::default()
    }
}

impl Files {
    async fn read(&self, level: i32, min: u64, max: u64, offset: u64, len: u64) -> Result<Bytes> {
        if let Some((seg, bytes)) = &self.overlay {
            if (seg.level as i32, seg.min, seg.max) == (level, min, max) {
                let start = (offset as usize).min(bytes.len());
                let end = (start + len as usize).min(bytes.len());
                return Ok(bytes.slice(start..end));
            }
        }
        let seg = self
            .by_range
            .get(&(level, min, max))
            .ok_or_else(|| anyhow!("no segment {level}/{min}-{max} in the plan"))?;
        self.catalog.fetch_range(seg, offset, len).await
    }
}

fn ltx_err(e: anyhow::Error) -> celld_ltx::Error {
    celld_ltx::Error::Other(format!("{e:#}").into())
}

struct Client(Arc<Files>);

#[async_trait]
impl ReplicaClient for Client {
    async fn ltx_files(&self, _level: i32, _seek: TXID) -> celld_ltx::Result<Vec<FileInfo>> {
        Ok(Vec::new())
    }

    async fn open_ltx_file(&self, level: i32, min: TXID, max: TXID) -> celld_ltx::Result<Vec<u8>> {
        self.read_range(level, min, max, 0, u64::MAX / 2).await
    }

    async fn read_range(
        &self,
        level: i32,
        min: TXID,
        max: TXID,
        offset: u64,
        len: u64,
    ) -> celld_ltx::Result<Vec<u8>> {
        self.0
            .read(level, min.0, max.0, offset, len)
            .await
            .map(|b| b.to_vec())
            .map_err(ltx_err)
    }

    async fn write_ltx_file(&self, _: i32, _: TXID, _: TXID, _: &[u8]) -> celld_ltx::Result<FileInfo> {
        Err(celld_ltx::Error::Other("read only".into()))
    }

    async fn delete_ltx_files(&self, _: &[FileInfo]) -> celld_ltx::Result<()> {
        Err(celld_ltx::Error::Other("read only".into()))
    }

    async fn delete_all(&self) -> celld_ltx::Result<()> {
        Err(celld_ltx::Error::Other("read only".into()))
    }
}

/// The VFS fault path: SQLite reads synchronously, so a fault blocks its
/// thread on the async read. Callers run SQLite on blocking threads
/// ([`PagedDb::blocking`]), never on a runtime worker.
struct Reader(Arc<Files>);

impl RangeReader for Reader {
    fn read_range(&self, info: &FileInfo, offset: u64, len: u64) -> celld_ltx::Result<Vec<u8>> {
        let files = self.0.clone();
        let (level, min, max) = (info.level, info.min_txid.0, info.max_txid.0);
        self.0
            .handle
            .block_on(async move { files.read(level, min, max, offset, len).await })
            .map(|b| b.to_vec())
            .map_err(ltx_err)
    }
}

/// The VFS's page source: one page per fault, or read-ahead runs in scan
/// mode.
struct Pages {
    map: PageMap,
    files: Arc<Files>,
    scan: AtomicBool,
}

impl Pages {
    fn one(&self, pgno: u32) -> celld_ltx::Result<Option<Vec<u8>>> {
        let client = Client(self.files.clone());
        self.files.handle.block_on(self.map.read_page(&client, pgno))
    }
}

impl PageReader for Pages {
    fn page_size(&self) -> u32 {
        self.map.page_size
    }

    fn commit(&self) -> u32 {
        self.map.commit
    }

    fn read_run(&self, pgno: u32) -> celld_ltx::Result<Vec<(u32, Vec<u8>)>> {
        if self.scan.load(Ordering::Relaxed) {
            return self.map.read_run_sync(&Reader(self.files.clone()), pgno, READAHEAD_BYTES);
        }
        Ok(self.one(pgno)?.map(|p| vec![(pgno, p)]).unwrap_or_default())
    }
}

pub struct PagedDb {
    conn: Option<Connection>,
    source: Option<Arc<Pages>>,
    vfs: Option<String>,
    /// Pages in the database at this position.
    pub commit: u32,
    _dir: TempDir,
}

impl PagedDb {
    /// The state after applying `plan`, then `overlay` if given. An empty
    /// plan with no overlay is the empty database.
    pub async fn open(catalog: Arc<Catalog>, plan: &[Segment], overlay: Option<(Segment, Bytes)>) -> Result<Self> {
        let dir = tempfile::Builder::new().prefix("celld-cdc-").tempdir()?;
        let path = dir.path().join("db.sqlite");
        let mut infos: Vec<FileInfo> = plan.iter().map(info).collect();
        if let Some((seg, bytes)) = &overlay {
            let mut i = info(seg);
            i.size = bytes.len() as i64;
            infos.push(i);
        }
        if infos.is_empty() {
            std::fs::File::create(&path)?;
            let conn = crate::diff::open(&path)?;
            return Ok(Self {
                conn: Some(conn),
                source: None,
                vfs: None,
                commit: 0,
                _dir: dir,
            });
        }
        let files = Arc::new(Files {
            catalog,
            by_range: plan.iter().map(|s| ((s.level as i32, s.min, s.max), s.clone())).collect(),
            overlay,
            handle: Handle::current(),
        });
        let map = build_page_map(&Client(files.clone()), &infos)
            .await
            .context("build page map")?;
        let commit = map.commit;
        let source = Arc::new(Pages {
            map,
            files,
            scan: AtomicBool::new(false),
        });
        let name = paged_vfs::next_registration_name();
        paged_vfs::register_paged_vfs(&name, None, &path, source.clone() as Arc<dyn PageReader>)
            .context("register paged vfs")?;
        // Opening reads page 1 through the VFS, which may fault.
        let vfs = name.clone();
        let conn = tokio::task::spawn_blocking(move || {
            Connection::open_with_flags_and_vfs(
                &path,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                &vfs,
            )
        })
        .await?
        .context("open paged database")?;
        Ok(Self {
            conn: Some(conn),
            source: Some(source),
            vfs: Some(name),
            commit,
            _dir: dir,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.source.is_none()
    }

    pub fn conn(&self) -> &Connection {
        self.conn.as_ref().expect("open until dropped")
    }

    /// One page's bytes at this position, fetched without SQLite; `None` for
    /// a page past the end or never written. Blocking.
    pub fn page(&self, pgno: u32) -> Result<Option<Vec<u8>>> {
        match &self.source {
            None => Ok(None),
            Some(s) => s.one(pgno).map_err(|e| anyhow!("read page {pgno}: {e}")),
        }
    }

    /// Read ahead on faults, for a query that reads whole tables.
    pub fn scan(&self, on: bool) {
        if let Some(s) = &self.source {
            s.scan.store(on, Ordering::Relaxed);
        }
    }
}

impl Drop for PagedDb {
    fn drop(&mut self) {
        drop(self.conn.take());
        if let Some(name) = &self.vfs {
            let _ = paged_vfs::unregister_paged_vfs(name);
        }
    }
}
