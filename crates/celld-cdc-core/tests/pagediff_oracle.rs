//! The page diff against its oracle, on random workloads.
//!
//! Each workload runs random transactions against a real SQLite database
//! and captures every commit as an LTX segment of the pages it changed, the
//! way celld captures a WAL. Then a lane runs over those segments twice,
//! once per diff mode, and the events must be identical, and folding them
//! must reproduce every table's final rows exactly.
//!
//! The workloads cover small and overflowing rows, page splits and merges,
//! deletes that free pages, `REPLACE` (a row that keeps its key and changes
//! rowid), a table without a key, a WITHOUT ROWID table, the `_cf_KV` table,
//! secondary indexes, schema changes, and VACUUM.

use anyhow::Result;
use async_trait::async_trait;
use celld_cdc_core::bucket::Bucket;
use celld_cdc_core::catalog::Catalog;
use celld_cdc_core::cursor::CursorStore;
use celld_cdc_core::diff;
use celld_cdc_core::lane::{Decoder, DiffMode, EventSink, Outcome, Step};
use celld_cdc_core::{ChangeEvent, Op};
use celld_ltx::ltx::{encode_file, Header, HEADER_FLAG_NO_CHECKSUM, VERSION};
use celld_ltx::TXID;
use rusqlite::{params, Connection};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const SCOPE: &str = "Fuzz:00";
const PAGE: usize = 4096;

/// xorshift64*: deterministic per seed, no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn text(&mut self, max: u64) -> String {
        let len = self.below(max) as usize;
        (0..len).map(|_| (b'a' + self.below(26) as u8) as char).collect()
    }
}

/// Random transactions; returns the LTX segment of each commit.
fn workload(seed: u64, dir: &Path) -> Vec<Vec<u8>> {
    let path = dir.join("source.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA page_size = 4096; PRAGMA journal_mode = DELETE; PRAGMA auto_vacuum = NONE;
         CREATE TABLE t1(id INTEGER PRIMARY KEY, a TEXT, b BLOB);
         CREATE INDEX t1_a ON t1(a);
         CREATE TABLE t2(k TEXT PRIMARY KEY, v INTEGER);
         CREATE TABLE t3(k TEXT PRIMARY KEY, v) WITHOUT ROWID;
         CREATE TABLE t4(x, y);
         CREATE TABLE _cf_KV(scope TEXT, k TEXT, v TEXT, PRIMARY KEY(scope, k));
         CREATE TABLE _cf_METADATA(scope TEXT PRIMARY KEY, actor_name TEXT);",
    )
    .unwrap();
    conn.execute("INSERT INTO _cf_METADATA VALUES (?1, 'fuzz')", [SCOPE]).unwrap();
    let mut rng = Rng(seed | 1);
    let mut pages: Vec<Vec<u8>> = Vec::new();
    let mut segments = Vec::new();
    let mut capture = |txid: u64, segments: &mut Vec<Vec<u8>>| {
        let file = std::fs::read(&path).unwrap();
        let now: Vec<Vec<u8>> = file.chunks(PAGE).map(<[u8]>::to_vec).collect();
        let changed: Vec<(u32, Vec<u8>)> = now
            .iter()
            .enumerate()
            .filter(|(i, p)| txid == 1 || pages.get(*i) != Some(*p))
            .map(|(i, p)| (i as u32 + 1, p.clone()))
            .collect();
        let header = Header {
            version: VERSION,
            flags: HEADER_FLAG_NO_CHECKSUM,
            page_size: PAGE as u32,
            commit: now.len() as u32,
            min_txid: TXID(txid),
            max_txid: TXID(txid),
            timestamp: 1_800_000_000_000 + txid as i64,
            ..Default::default()
        };
        segments.push(encode_file(&header, &changed, 0).unwrap());
        pages = now;
    };
    capture(1, &mut segments);

    for txid in 2..=60u64 {
        let roll = rng.below(100);
        if roll < 3 {
            conn.execute_batch("VACUUM").unwrap();
        } else if roll < 5 {
            let col = format!("c{txid}");
            conn.execute_batch(&format!("ALTER TABLE t4 ADD COLUMN {col} DEFAULT 7")).unwrap();
        } else {
            let tx = conn.unchecked_transaction().unwrap();
            for _ in 0..1 + rng.below(6) {
                let id = rng.below(400) as i64;
                match rng.below(12) {
                    0 | 1 => {
                        // Sometimes bigger than a page: an overflow chain.
                        let big = rng.below(10) == 0;
                        let blob = vec![rng.below(256) as u8; if big { 9000 } else { rng.below(300) as usize }];
                        tx.execute(
                            "INSERT OR REPLACE INTO t1(id, a, b) VALUES (?1, ?2, ?3)",
                            params![id, rng.text(40), blob],
                        )
                        .unwrap();
                    }
                    2 => {
                        tx.execute("UPDATE t1 SET a = ?2 WHERE id = ?1", params![id, rng.text(200)]).unwrap();
                    }
                    3 => {
                        tx.execute("DELETE FROM t1 WHERE id BETWEEN ?1 AND ?1 + 20", [id]).unwrap();
                    }
                    4 => {
                        // Many rows at once: page splits.
                        for i in 0..60 {
                            tx.execute(
                                "INSERT OR IGNORE INTO t1(id, a) VALUES (?1, ?2)",
                                params![1000 + id * 60 + i, rng.text(80)],
                            )
                            .unwrap();
                        }
                    }
                    5 => {
                        // REPLACE keeps the key and gives the row a new rowid.
                        tx.execute(
                            "REPLACE INTO t2(k, v) VALUES (?1, ?2)",
                            params![format!("k{}", rng.below(30)), rng.next() as i64],
                        )
                        .unwrap();
                    }
                    6 => {
                        tx.execute("DELETE FROM t2 WHERE k = ?1", [format!("k{}", rng.below(30))]).unwrap();
                    }
                    7 => {
                        tx.execute(
                            "INSERT OR REPLACE INTO t3(k, v) VALUES (?1, ?2)",
                            params![format!("w{}", rng.below(50)), rng.text(100)],
                        )
                        .unwrap();
                    }
                    8 => {
                        tx.execute("INSERT INTO t4(x, y) VALUES (?1, ?2)", params![rng.text(20), id]).unwrap();
                    }
                    9 => {
                        tx.execute("DELETE FROM t4 WHERE rowid % 3 = ?1", [rng.below(3) as i64]).unwrap();
                    }
                    _ => {
                        tx.execute(
                            "INSERT OR REPLACE INTO _cf_KV(scope, k, v) VALUES (?1, ?2, ?3)",
                            params![SCOPE, format!("key{}", rng.below(20)), format!("{}", rng.below(1_000_000))],
                        )
                        .unwrap();
                    }
                }
            }
            tx.commit().unwrap();
        }
        capture(txid, &mut segments);
    }
    segments
}

#[derive(Default)]
struct Collect(Mutex<Vec<ChangeEvent>>);

#[async_trait]
impl EventSink for Collect {
    async fn write(&self, _: &Step, events: &[ChangeEvent]) -> Result<()> {
        self.0.lock().unwrap().extend_from_slice(events);
        Ok(())
    }
}

async fn run(segments: &[Vec<u8>], mode: DiffMode) -> Vec<ChangeEvent> {
    let celld = Bucket::memory();
    for (i, seg) in segments.iter().enumerate() {
        let txid = i as u64 + 1;
        let key = format!("cells/{SCOPE}/ltx/e1/0000/{txid:016x}-{txid:016x}.ltx");
        celld.put(&key, seg.clone().into()).await.unwrap();
    }
    let cdc = Bucket::memory();
    let sink = Arc::new(Collect::default());
    let decoder = Decoder {
        catalog: Arc::new(Catalog::new(celld, cdc.clone())),
        cursors: CursorStore::new(cdc, "test", 30_000),
        sink: sink.clone(),
        mode,
    };
    let out = decoder.run_lane(SCOPE).await.unwrap();
    assert!(matches!(out, Outcome::CaughtUp { applied, .. } if applied == segments.len() as u64), "{out:?}");
    let events = sink.0.lock().unwrap().clone();
    events
}

/// Fold events into the final rows of each table, keyed like the diff.
fn fold(events: &[ChangeEvent]) -> BTreeMap<String, BTreeMap<String, Value>> {
    let mut tables: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for e in events {
        let rows = tables.entry(e.table.clone()).or_default();
        match e.op {
            Op::Delete => {
                rows.remove(&e.pk.to_string());
            }
            _ => {
                rows.insert(e.pk.to_string(), e.after.clone().unwrap());
            }
        }
    }
    tables.retain(|_, rows| !rows.is_empty());
    tables
}

fn truth(dir: &Path) -> BTreeMap<String, BTreeMap<String, Value>> {
    let conn = diff::open(&dir.join("source.db")).unwrap();
    let mut tables = BTreeMap::new();
    for t in ["t1", "t2", "t3", "t4", "_cf_KV"] {
        let rows: BTreeMap<String, Value> = diff::read_rows(&conn, t, None)
            .unwrap()
            .into_iter()
            .map(|(k, (_, row))| (k, row))
            .collect();
        if !rows.is_empty() {
            tables.insert(t.to_string(), rows);
        }
    }
    tables
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn page_diff_matches_the_full_diff_and_the_truth() {
    let paged_before = celld_cdc_core::pagediff::PAGED.load(Ordering::Relaxed);
    for seed in [1, 2, 3, 7, 42, 1234, 99991, 31337] {
        let dir = tempfile::tempdir().unwrap();
        let segments = workload(seed, dir.path());
        let paged = run(&segments, DiffMode::Paged).await;
        let full = run(&segments, DiffMode::Full).await;
        assert_eq!(paged.len(), full.len(), "seed {seed}: event counts");
        for (p, f) in paged.iter().zip(&full) {
            assert_eq!(p, f, "seed {seed}");
        }
        assert_eq!(fold(&paged), truth(dir.path()), "seed {seed}: folded events differ from the database");
    }
    let paged_runs = celld_cdc_core::pagediff::PAGED.load(Ordering::Relaxed) - paged_before;
    // Most steps must take the page path, not the fallback: 8 workloads of
    // 59 steps after the first, less VACUUMs and schema changes.
    assert!(paged_runs > 400, "only {paged_runs} diffs used pages");
}

/// A large actor with a one-row change: the paged lane must read a small
/// fraction of what a full rebuild reads, and produce the same event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_one_row_change_in_a_large_actor_reads_little() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA page_size = 4096; PRAGMA journal_mode = DELETE;
         CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT);
         CREATE TABLE _cf_METADATA(scope TEXT PRIMARY KEY, actor_name TEXT);",
    )
    .unwrap();
    // Random bodies, so LZ4 cannot shrink the snapshot to nothing.
    let mut rng = Rng(7);
    let tx = conn.unchecked_transaction().unwrap();
    for id in 0..40_000 {
        tx.execute("INSERT INTO t VALUES (?1, ?2)", params![id, rng.text(1000)]).unwrap();
    }
    tx.commit().unwrap();
    let snapshot = std::fs::read(&path).unwrap();
    conn.execute("UPDATE t SET body = 'changed' WHERE id = 31337", []).unwrap();
    let after = std::fs::read(&path).unwrap();
    let ltx = |txid: u64, pages: Vec<(u32, Vec<u8>)>, commit: usize| {
        let header = Header {
            version: VERSION,
            flags: HEADER_FLAG_NO_CHECKSUM,
            page_size: PAGE as u32,
            commit: commit as u32,
            min_txid: TXID(txid),
            max_txid: TXID(txid),
            timestamp: 1_800_000_000_000,
            ..Default::default()
        };
        encode_file(&header, &pages, 0).unwrap()
    };
    let all: Vec<(u32, Vec<u8>)> =
        snapshot.chunks(PAGE).enumerate().map(|(i, p)| (i as u32 + 1, p.to_vec())).collect();
    let changed: Vec<(u32, Vec<u8>)> = after
        .chunks(PAGE)
        .zip(snapshot.chunks(PAGE))
        .enumerate()
        .filter(|(_, (a, b))| a != b)
        .map(|(i, (a, _))| (i as u32 + 1, a.to_vec()))
        .collect();
    let segments = [ltx(1, all, snapshot.len() / PAGE), ltx(2, changed, after.len() / PAGE)];
    let db_mb = snapshot.len() as f64 / 1e6;

    let mut results = Vec::new();
    for mode in [DiffMode::Paged, DiffMode::Full] {
        let celld = Bucket::memory();
        for (i, seg) in segments.iter().enumerate() {
            let t = i as u64 + 1;
            celld
                .put(&format!("cells/{SCOPE}/ltx/e1/0000/{t:016x}-{t:016x}.ltx"), seg.clone().into())
                .await
                .unwrap();
        }
        let cdc = Bucket::memory();
        // The actor's cursor is already past the snapshot.
        let cursors = CursorStore::new(cdc.clone(), "test", 30_000);
        let celld_cdc_core::cursor::Claim::Held(mut held) = cursors.claim(SCOPE).await.unwrap() else {
            panic!("claim");
        };
        let pos = celld_cdc_core::cursor::Position { epoch: 1, txid: 1 };
        cursors.advance(&mut held, pos).await.unwrap();
        cursors.release(held).await.unwrap();
        let sink = Arc::new(Collect::default());
        let catalog = Arc::new(Catalog::new(celld, cdc.clone()));
        let decoder = Decoder {
            catalog: catalog.clone(),
            cursors: CursorStore::new(cdc, "test", 30_000),
            sink: sink.clone(),
            mode,
        };
        let started = std::time::Instant::now();
        decoder.run_lane(SCOPE).await.unwrap();
        let events = sink.0.lock().unwrap().clone();
        results.push((events, catalog.fetched_bytes(), started.elapsed()));
    }
    let (paged, full) = (&results[0], &results[1]);
    eprintln!(
        "{db_mb:.1} MB actor, one-row update: paged read {} KB in {:?}, full read {} KB in {:?}",
        paged.1 / 1000,
        paged.2,
        full.1 / 1000,
        full.2
    );
    assert_eq!(paged.0, full.0);
    assert_eq!(paged.0.len(), 1);
    assert_eq!(paged.0[0].after.as_ref().unwrap()["body"], "changed");
    assert!(paged.1 * 20 < full.1, "paged read {} of full {}", paged.1, full.1);
}
