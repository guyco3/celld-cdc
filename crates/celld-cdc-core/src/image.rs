//! A scratch SQLite file rebuilt from LTX segments.
//!
//! Applying a segment overwrites its pages and sizes the file to its
//! `commit` page count, the same merge celld's restore does. The file lives
//! in a temp directory and is never the source of truth: any worker can
//! rebuild it from the bucket.

use anyhow::{Context, Result};
use celld_ltx::ltx::{Header, HEADER_SIZE};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub struct DbImage {
    _dir: TempDir,
    path: PathBuf,
}

impl DbImage {
    /// An empty database: zero bytes, which SQLite reads as no tables.
    pub fn empty() -> Result<Self> {
        let dir = tempfile::Builder::new().prefix("celld-cdc-").tempdir()?;
        let path = dir.path().join("db.sqlite");
        File::create(&path)?;
        Ok(Self { _dir: dir, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A copy to apply the next segment to while this one stays the
    /// "before" side of the diff.
    pub fn fork(&self) -> Result<Self> {
        let next = Self::empty()?;
        std::fs::copy(&self.path, &next.path)?;
        Ok(next)
    }

    /// Apply one LTX file and return its header.
    pub fn apply(&mut self, ltx: &[u8]) -> Result<Header> {
        let header = Header::parse(ltx.get(..HEADER_SIZE).context("ltx shorter than header")?)
            .context("parse ltx header")?;
        let pages = celld_ltx::ltx::decode_file_pages(ltx).context("decode ltx pages")?;
        let file = OpenOptions::new().write(true).open(&self.path)?;
        let size = header.page_size as u64;
        for (pgno, mut data) in pages {
            if pgno == 1 {
                rollback_journal_mode(&mut data);
            }
            file.write_all_at(&data, (pgno as u64 - 1) * size)?;
        }
        file.set_len(header.commit as u64 * size)?;
        Ok(header)
    }

    /// Apply segments in order; the header of the last one, if any.
    pub fn apply_all<'a>(&mut self, ltxs: impl IntoIterator<Item = &'a [u8]>) -> Result<Option<Header>> {
        let mut last = None;
        for ltx in ltxs {
            last = Some(self.apply(ltx)?);
        }
        Ok(last)
    }
}

/// celld's databases are in WAL mode (file format bytes 18 and 19 are 2).
/// The image has no `-wal` file, so mark it rollback-journal (1) and SQLite
/// reads the main file alone.
fn rollback_journal_mode(page1: &mut [u8]) {
    if page1.len() > 19 && page1.starts_with(b"SQLite format 3\0") {
        page1[18] = 1;
        page1[19] = 1;
    }
}
