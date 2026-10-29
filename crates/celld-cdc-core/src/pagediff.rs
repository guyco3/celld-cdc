//! Row-level diff from the pages a segment changed.
//!
//! A segment names every page it wrote, so the rows that changed live on
//! those pages. For each captured table, the diff finds which changed pages
//! are leaves of its b-tree (walking interior pages only; the leaves sit at
//! one depth, so one read per level tells interior from leaf), reads the
//! rowids off the old and new version of each such leaf, and has SQLite
//! read just those rows on both sides. A row a page split moved shows up on
//! two pages under one key, so it nets out. Cost follows the change, not the
//! database.
//!
//! It falls back to [`full_diff`] where pages are not a cheap guide: from an
//! empty database, after a schema change, when a large share of pages
//! changed (a VACUUM or a snapshot), and for WITHOUT ROWID tables, whose
//! leaves hold keys rather than rowids (those compare the whole table).
//! `full_diff` is the oracle: the two must always agree.

use crate::diff::{captured_tables, compare, full_diff, read_rows, RowChange};
use crate::paged::PagedDb;
use anyhow::{bail, Result};
use rusqlite::Connection;
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// Diffs answered from pages, and diffs that fell back to whole tables, in
/// this process: for operators and for tests that the fast path ran.
pub static PAGED: AtomicU64 = AtomicU64::new(0);
pub static FALLBACK: AtomicU64 = AtomicU64::new(0);

/// Above this share of the database's pages changed (and at least
/// [`FULL_DIFF_MIN_PAGES`]), a full diff is cheaper.
const FULL_DIFF_SHARE: f64 = 0.25;
const FULL_DIFF_MIN_PAGES: usize = 64;

const INTERIOR_INDEX: u8 = 0x02;
const INTERIOR_TABLE: u8 = 0x05;
const LEAF_INDEX: u8 = 0x0a;
const LEAF_TABLE: u8 = 0x0d;

/// The diff of `before` and `after`, which differ in `changed` pages.
/// Blocking: run it inside [`PagedDb::blocking`].
pub fn page_diff(before: &PagedDb, after: &PagedDb, changed: &[u32]) -> Result<Vec<RowChange>> {
    let out = try_page_diff(before, after, changed)?;
    match out {
        Some(changes) => {
            PAGED.fetch_add(1, Ordering::Relaxed);
            Ok(changes)
        }
        None => {
            FALLBACK.fetch_add(1, Ordering::Relaxed);
            before.scan(true);
            after.scan(true);
            full_diff(before.conn(), after.conn())
        }
    }
}

/// The page-guided diff, or `None` where a full diff is the right tool.
fn try_page_diff(before: &PagedDb, after: &PagedDb, changed: &[u32]) -> Result<Option<Vec<RowChange>>> {
    let (b, a) = (before.conn(), after.conn());
    let mostly_changed =
        changed.len() > FULL_DIFF_MIN_PAGES && changed.len() as f64 > FULL_DIFF_SHARE * after.commit as f64;
    if before.is_empty() || mostly_changed {
        return Ok(None);
    }
    let schema = schema(a)?;
    if schema != self::schema(b)? {
        return Ok(None);
    }
    let changed: HashSet<u32> = changed.iter().copied().collect();
    let mut out = Vec::new();
    let mut tables = captured_tables(a)?;
    // The same order as the full diff, so the two agree event for event.
    tables.sort();
    for table in tables {
        let Some(t) = schema.iter().find(|t| t.name == table) else {
            continue;
        };
        let (old, new) = match (tree(before, t.root), tree(after, t.root)) {
            (Ok(old), Ok(new)) => (old, new),
            // An unreadable tree is not a guess this diff makes.
            _ => return Ok(None),
        };
        let touched = |tree: &Tree| changed.iter().any(|p| tree.interior.contains(p) || tree.leaves.contains(p));
        if !touched(&old) && !touched(&new) {
            continue;
        }
        if t.without_rowid {
            before.scan(true);
            after.scan(true);
            compare(&table, read_rows(b, &table, None)?, read_rows(a, &table, None)?, &mut out);
            before.scan(false);
            after.scan(false);
            continue;
        }
        let mut rowids = BTreeSet::new();
        for (db, tree) in [(before, &old), (after, &new)] {
            for &pgno in changed.iter().filter(|p| tree.leaves.contains(p)) {
                if let Some(page) = db.page(pgno)? {
                    rowids.extend(leaf_rowids(&page, pgno)?);
                }
            }
        }
        let ids: Vec<i64> = rowids.into_iter().collect();
        compare(&table, read_rows(b, &table, Some(&ids))?, read_rows(a, &table, Some(&ids))?, &mut out);
    }
    Ok(Some(out))
}

#[derive(Debug, PartialEq, Eq)]
struct SchemaTable {
    name: String,
    root: u32,
    without_rowid: bool,
    sql: String,
}

/// Every table and index entry of `sqlite_schema`; equal on both sides means
/// no table moved or changed shape.
fn schema(conn: &Connection) -> Result<Vec<SchemaTable>> {
    let mut stmt = conn.prepare(
        "SELECT type, name, coalesce(rootpage, 0), coalesce(sql, '') FROM sqlite_schema ORDER BY type, name",
    )?;
    let rows = stmt.query_map([], |r| {
        let kind: String = r.get(0)?;
        let sql: String = r.get(3)?;
        Ok(SchemaTable {
            name: if kind == "table" { r.get(1)? } else { format!("{kind}:{}", r.get::<_, String>(1)?) },
            root: r.get::<_, i64>(2)? as u32,
            without_rowid: sql.to_ascii_uppercase().contains("WITHOUT ROWID"),
            sql,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// A b-tree's interior pages and leaves.
#[derive(Default)]
struct Tree {
    interior: HashSet<u32>,
    leaves: HashSet<u32>,
}

/// Walk a b-tree from `root`, reading interior pages only: every leaf is at
/// the same depth, so the first page of a level says whether the level is
/// leaves.
fn tree(db: &PagedDb, root: u32) -> Result<Tree> {
    let mut tree = Tree::default();
    if root == 0 {
        return Ok(tree);
    }
    let mut level = vec![root];
    for _ in 0..64 {
        let Some(first) = db.page(level[0])? else {
            return Ok(tree);
        };
        match page_type(&first, level[0])? {
            LEAF_TABLE | LEAF_INDEX => {
                tree.leaves.extend(level);
                return Ok(tree);
            }
            INTERIOR_TABLE | INTERIOR_INDEX => {
                let mut next = Vec::new();
                for &pgno in &level {
                    let page = if pgno == level[0] { first.clone() } else { db.page(pgno)?.unwrap_or_default() };
                    next.extend(children(&page, pgno)?);
                }
                tree.interior.extend(level);
                if next.is_empty() {
                    return Ok(tree);
                }
                level = next;
            }
            t => bail!("page {} has b-tree type {t:#x}", level[0]),
        }
    }
    bail!("b-tree at {root} is deeper than 64 levels")
}

/// Where the b-tree header starts: page 1 begins with the 100-byte file
/// header.
fn header_at(pgno: u32) -> usize {
    if pgno == 1 {
        100
    } else {
        0
    }
}

fn page_type(page: &[u8], pgno: u32) -> Result<u8> {
    match page.get(header_at(pgno)) {
        Some(t) => Ok(*t),
        None => bail!("page {pgno} is empty"),
    }
}

fn u16_at(page: &[u8], at: usize) -> Result<usize> {
    match page.get(at..at + 2) {
        Some(b) => Ok(u16::from_be_bytes([b[0], b[1]]) as usize),
        None => bail!("truncated page"),
    }
}

fn u32_at(page: &[u8], at: usize) -> Result<u32> {
    match page.get(at..at + 4) {
        Some(b) => Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]])),
        None => bail!("truncated page"),
    }
}

/// The offsets of a page's cells, from its cell pointer array.
fn cells(page: &[u8], pgno: u32) -> Result<Vec<usize>> {
    let h = header_at(pgno);
    let interior = matches!(page_type(page, pgno)?, INTERIOR_TABLE | INTERIOR_INDEX);
    let count = u16_at(page, h + 3)?;
    let array = h + if interior { 12 } else { 8 };
    (0..count).map(|i| u16_at(page, array + 2 * i)).collect()
}

/// An interior page's child pages: each cell's left child, then the
/// right-most pointer.
fn children(page: &[u8], pgno: u32) -> Result<Vec<u32>> {
    let mut out = Vec::new();
    for at in cells(page, pgno)? {
        out.push(u32_at(page, at)?);
    }
    out.push(u32_at(page, header_at(pgno) + 8)?);
    Ok(out)
}

/// The rowids on a table leaf: each cell is `varint payload_len, varint
/// rowid, payload`.
fn leaf_rowids(page: &[u8], pgno: u32) -> Result<Vec<i64>> {
    if page_type(page, pgno)? != LEAF_TABLE {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for at in cells(page, pgno)? {
        let (_, n) = varint(page, at)?;
        let (rowid, _) = varint(page, at + n)?;
        out.push(rowid as i64);
    }
    Ok(out)
}

/// SQLite's big-endian varint: 7 bits a byte for up to eight bytes, then a
/// full ninth byte. Returns the value and its length.
fn varint(page: &[u8], at: usize) -> Result<(u64, usize)> {
    let mut v: u64 = 0;
    for i in 0..9 {
        let Some(&b) = page.get(at + i) else {
            bail!("truncated varint");
        };
        if i == 8 {
            return Ok(((v << 8) | b as u64, 9));
        }
        v = (v << 7) | (b & 0x7f) as u64;
        if b & 0x80 == 0 {
            return Ok((v, i + 1));
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_varints() {
        assert_eq!(varint(&[0x05], 0).unwrap(), (5, 1));
        assert_eq!(varint(&[0x81, 0x00], 0).unwrap(), (128, 2));
        assert_eq!(varint(&[0xff; 9], 0).unwrap(), (u64::MAX, 9));
    }
}
