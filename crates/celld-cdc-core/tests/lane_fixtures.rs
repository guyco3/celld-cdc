//! Run lanes over objects captured from a real two-node celld fleet running
//! `examples/counter`:
//!
//! - alice: four single-node L0s (`n` = 1..4), then six writes that live
//!   only in a node-log bundle (`n` = 5..10);
//! - bob: one L0 with a `clicks` row, `n` = 5, and a `last` object;
//! - carol: six writes, bundle only (`n` = 1..6).

use anyhow::Result;
use async_trait::async_trait;
use celld_cdc_core::bucket::Bucket;
use celld_cdc_core::catalog::{self, Catalog};
use celld_cdc_core::cursor::CursorStore;
use celld_cdc_core::lane::{Decoder, DiffMode, EventSink, Outcome, Step};
use celld_cdc_core::{ChangeEvent, Op};
use serde_json::json;
use std::path::Path;
use std::sync::{Arc, Mutex};

const ALICE: &str = "Counter:1182fbf2bee1753c4ffbfddea36c7eff522bfe47006c0f9f7e6035db0fc0b40d";
const BOB: &str = "Counter:399ba7b91e36985f42ae178e0855eea6a0a14b71a6fc5dbb32f646bba798add8";
const CAROL: &str = "Counter:c4161385248524ce3a0041d4962209096d2088bebdb652fc011e5c89e8394ac0";

#[derive(Default)]
struct Collect(Mutex<Vec<(Step, Vec<ChangeEvent>)>>);

#[async_trait]
impl EventSink for Collect {
    async fn write(&self, step: &Step, events: &[ChangeEvent]) -> Result<()> {
        self.0.lock().unwrap().push((step.clone(), events.to_vec()));
        Ok(())
    }
}

async fn load_fixtures() -> Bucket {
    let bucket = Bucket::memory();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bucket");
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let key = path.strip_prefix(&root).unwrap().to_str().unwrap().to_string();
                bucket.put(&key, std::fs::read(&path).unwrap().into()).await.unwrap();
            }
        }
    }
    bucket
}

fn decoder(celld: Bucket, cdc: Bucket, sink: Arc<Collect>) -> Decoder {
    decoder_with(celld, cdc, sink, DiffMode::Paged)
}

fn decoder_with(celld: Bucket, cdc: Bucket, sink: Arc<Collect>, mode: DiffMode) -> Decoder {
    Decoder {
        catalog: Arc::new(Catalog::new(celld, cdc.clone())),
        cursors: CursorStore::new(cdc, "test", 30_000),
        sink,
        mode,
    }
}

fn n_values(events: &[(Step, Vec<ChangeEvent>)]) -> Vec<serde_json::Value> {
    events
        .iter()
        .flat_map(|(_, e)| e)
        .filter(|e| e.table == "_cf_KV" && e.pk == json!({"k": "n"}))
        .map(|e| e.after.as_ref().unwrap()["v"].clone())
        .collect()
}

#[tokio::test]
async fn alice_history_spans_cell_objects_and_a_bundle() {
    let (celld, cdc) = (load_fixtures().await, Bucket::memory());
    let sink = Arc::new(Collect::default());
    let d = decoder(celld, cdc, sink.clone());
    let out = d.run_lane(ALICE).await.unwrap();
    assert_eq!(
        out,
        Outcome::CaughtUp {
            pos: Some(celld_cdc_core::cursor::Position { epoch: 1, txid: 10 }),
            applied: 10
        }
    );
    let events = sink.0.lock().unwrap();
    assert_eq!(n_values(&events), (1..=10).map(|n| json!(n)).collect::<Vec<_>>());
    let first = &events[0].1.iter().find(|e| e.table == "_cf_KV").unwrap();
    assert_eq!(first.op, Op::Create);
    assert_eq!(first.actor_name.as_deref(), Some("alice"));
    // Every event after the first segment is an update of `n`.
    assert!(events[1..].iter().all(|(_, e)| e.len() == 1 && e[0].op == Op::Update));
    assert!(events[4].0.source_key.ends_with(".ltxb"));
}

#[tokio::test]
async fn bob_sees_the_sql_row_and_the_kv_object() {
    let (celld, cdc) = (load_fixtures().await, Bucket::memory());
    let sink = Arc::new(Collect::default());
    decoder(celld, cdc, sink.clone()).run_lane(BOB).await.unwrap();
    let events = sink.0.lock().unwrap();
    let all: Vec<_> = events.iter().flat_map(|(_, e)| e).collect();
    let click = all.iter().find(|e| e.table == "clicks").unwrap();
    assert_eq!(click.pk, json!({"id": 1}));
    assert_eq!(click.after.as_ref().unwrap()["delta"], json!(5));
    assert_eq!(click.after.as_ref().unwrap()["note"], json!("hi"));
    let last = all.iter().find(|e| e.pk == json!({"k": "last"})).unwrap();
    assert_eq!(last.after.as_ref().unwrap()["v"]["delta"], json!(5));
    // `sqlite_sequence` (AUTOINCREMENT) is SQLite's own bookkeeping.
    assert!(all.iter().all(|e| !e.table.starts_with("sqlite_")));
}

#[tokio::test]
async fn a_rerun_is_idempotent_and_archives_bundle_segments() {
    let (celld, cdc) = (load_fixtures().await, Bucket::memory());
    let sink = Arc::new(Collect::default());
    let d = decoder(celld.clone(), cdc.clone(), sink.clone());
    d.run_lane(CAROL).await.unwrap();
    assert_eq!(n_values(&sink.0.lock().unwrap()), (1..=6).map(|n| json!(n)).collect::<Vec<_>>());
    // A second doorbell finds nothing new.
    let again = d.run_lane(CAROL).await.unwrap();
    assert!(matches!(again, Outcome::CaughtUp { applied: 0, .. }));
    // The bundle drains away; the archive keeps carol's state rebuildable.
    for (k, _) in celld.list("log/").await.unwrap() {
        celld.delete(&k).await.unwrap();
    }
    let archived = cdc.list("segments/").await.unwrap();
    assert_eq!(archived.len(), 6);
    assert!(archived[0].0.starts_with(&format!("segments/{CAROL}/e1/")));
    let fresh = Arc::new(Collect::default());
    let d2 = decoder(celld, Bucket::memory(), fresh.clone());
    // A new cdc bucket without the archive cannot see carol at all now,
    // which is why the archive exists.
    assert!(matches!(d2.run_lane(CAROL).await.unwrap(), Outcome::CaughtUp { pos: None, .. }));
    let _ = catalog::archive_key(CAROL, 1, 1, 1);
}

#[tokio::test]
async fn doorbell_keys_name_their_actors() {
    let celld = load_fixtures().await;
    let cat = Catalog::new(celld.clone(), Bucket::memory());
    let ltx = format!("cells/{ALICE}/ltx/e1/0000/0000000000000002-0000000000000002.ltx");
    assert_eq!(catalog::scopes_for_key(&cat, &ltx).await.unwrap(), vec![ALICE]);
    let (bundle, _) = celld.list("log/").await.unwrap().remove(0);
    let mut scopes = catalog::scopes_for_key(&cat, &bundle).await.unwrap();
    scopes.sort();
    assert_eq!(scopes, vec![ALICE.to_string(), CAROL.to_string()]);
}

#[tokio::test]
async fn paged_and_full_diffs_agree_on_the_fixtures() {
    for scope in [ALICE, BOB, CAROL] {
        let mut runs = Vec::new();
        for mode in [DiffMode::Paged, DiffMode::Full] {
            let sink = Arc::new(Collect::default());
            decoder_with(load_fixtures().await, Bucket::memory(), sink.clone(), mode)
                .run_lane(scope)
                .await
                .unwrap();
            let events: Vec<ChangeEvent> = sink.0.lock().unwrap().iter().flat_map(|(_, e)| e.clone()).collect();
            runs.push(events);
        }
        assert!(!runs[0].is_empty());
        assert_eq!(runs[0], runs[1], "{scope}");
    }
}
