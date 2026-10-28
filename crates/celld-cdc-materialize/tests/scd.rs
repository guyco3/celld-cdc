//! End-to-end SCD1/SCD2 behaviour against local Delta tables.

use std::collections::{BTreeSet, HashMap};

use celld_cdc_core::event::{event_id, seq};
use celld_cdc_core::{ChangeEvent, Op};
use celld_cdc_materialize::{ApplyStats, Materializer, Scd1Row, Scd2Row};
use serde_json::{json, Value};
use tempfile::TempDir;

const SCOPE: &str = "Counter:00ab";

fn ev(scope: &str, table: &str, id: i64, txid: u64, op: Op, after: Option<Value>) -> ChangeEvent {
    let pk = json!({ "id": id });
    ChangeEvent {
        event_id: event_id(scope, 1, txid, table, &pk),
        class: scope.split(':').next().unwrap().into(),
        scope: scope.into(),
        actor_name: Some("main".into()),
        table: table.into(),
        op,
        epoch: 1,
        min_txid: txid,
        txid,
        seq: seq(1, txid),
        commit_ts_ms: 1_700_000_000_000 + txid as i64,
        source_key: format!("cells/{scope}/{txid}.ltx"),
        pk,
        before: None,
        after,
    }
}

/// Row 1: create (t2), update (t3), delete (t5). Row 2: create (t4).
/// Plus one row of another table and another scope.
fn history() -> Vec<ChangeEvent> {
    vec![
        ev(SCOPE, "clicks", 1, 2, Op::Create, Some(json!({"id": 1, "n": 1}))),
        ev(SCOPE, "clicks", 1, 3, Op::Update, Some(json!({"id": 1, "n": 2}))),
        ev(SCOPE, "clicks", 2, 4, Op::Create, Some(json!({"id": 2, "n": 9}))),
        ev(SCOPE, "clicks", 1, 5, Op::Delete, None),
        ev(SCOPE, "views", 1, 4, Op::Create, Some(json!({"id": 1, "v": "x"}))),
        ev("Room:0cd", "clicks", 1, 7, Op::Create, Some(json!({"id": 1}))),
    ]
}

async fn open(dir: &TempDir) -> Materializer {
    Materializer::open(dir.path().to_str().unwrap(), HashMap::new()).await.unwrap()
}

async fn contents(m: &mut Materializer) -> (Vec<Scd1Row>, Vec<Scd2Row>) {
    (m.read_scd1().await.unwrap(), m.read_scd2(None).await.unwrap())
}

fn versions<'a>(rows: &'a [Scd2Row], table: &str, pk: &str) -> Vec<&'a Scd2Row> {
    let mut v: Vec<_> =
        rows.iter().filter(|r| r.scope == SCOPE && r.table_name == table && r.pk == pk).collect();
    v.sort_by(|a, b| a.valid_from_seq.cmp(&b.valid_from_seq));
    v
}

#[tokio::test]
async fn empty_tables_and_empty_apply() {
    let dir = TempDir::new().unwrap();
    let mut m = open(&dir).await;
    assert_eq!(contents(&mut m).await, (vec![], vec![]));
    assert_eq!(m.apply(vec![]).await.unwrap(), ApplyStats::default());
    let keys = BTreeSet::from([("C".into(), "t".into(), "C:1".into(), "{}".into())]);
    assert!(m.read_scd2(Some(&keys)).await.unwrap().is_empty());
}

#[tokio::test]
async fn scd1_and_scd2_contents() {
    let dir = TempDir::new().unwrap();
    let mut m = open(&dir).await;
    let stats = m.apply(history()).await.unwrap();
    assert_eq!(
        stats,
        ApplyStats {
            events: 6,
            unique_events: 6,
            keys: 4,
            scd1_inserted: 4,
            scd1_updated: 0,
            scd2_inserted: 6,
            scd2_updated: 0,
        }
    );
    let (scd1, scd2) = contents(&mut m).await;

    let pk1 = json!({"id": 1}).to_string();
    let r1 = scd1.iter().find(|r| r.scope == SCOPE && r.table_name == "clicks" && r.pk == pk1).unwrap();
    assert!(r1.is_deleted);
    assert_eq!(r1.row, None);
    assert_eq!(r1.op, "d");
    assert_eq!(r1.seq, seq(1, 5));
    let pk2 = json!({"id": 2}).to_string();
    let r2 = scd1.iter().find(|r| r.table_name == "clicks" && r.pk == pk2).unwrap();
    assert_eq!(r2.row.as_deref(), Some(r#"{"id":2,"n":9}"#));
    assert!(!r2.is_deleted);
    let j = serde_json::to_value(r2).unwrap();
    assert_eq!((j["table_name"].as_str(), j["is_deleted"].as_bool()), (Some("clicks"), Some(false)));

    let v = versions(&scd2, "clicks", &pk1);
    assert_eq!(v.len(), 3);
    assert_eq!(v.iter().map(|r| r.op.as_str()).collect::<Vec<_>>(), ["c", "u", "d"]);
    assert_eq!(v[0].valid_to_seq.as_deref(), Some(seq(1, 3).as_str()));
    assert_eq!(v[0].valid_to_ts_ms, Some(v[1].valid_from_ts_ms));
    assert_eq!(v[1].valid_to_seq.as_deref(), Some(seq(1, 5).as_str()));
    assert_eq!(v[2].valid_to_seq, None);
    assert_eq!(v.iter().map(|r| r.is_current).collect::<Vec<_>>(), [false, false, true]);
    assert!(v[2].is_deleted && v[2].row.is_none());
    assert_eq!(scd2.iter().filter(|r| r.is_current).count(), 4);
    assert_eq!(serde_json::to_value(v[2]).unwrap()["valid_to_seq"], Value::Null);

    // Reopening sees the same tables.
    drop(m);
    let mut m = open(&dir).await;
    assert_eq!(contents(&mut m).await, (scd1, scd2));
}

#[tokio::test]
async fn reapply_is_a_no_op() {
    let dir = TempDir::new().unwrap();
    let mut m = open(&dir).await;
    m.apply(history()).await.unwrap();
    let before = contents(&mut m).await;
    let v = m.scd2().version();
    let stats = m.apply(history()).await.unwrap();
    assert_eq!((stats.scd1_inserted, stats.scd1_updated), (0, 0));
    assert_eq!((stats.scd2_inserted, stats.scd2_updated), (0, 0));
    assert_eq!(m.scd2().version(), v, "nothing changed, so no SCD2 commit");
    assert_eq!(contents(&mut m).await, before);
}

#[tokio::test]
async fn order_batching_and_duplicates_converge() {
    let expected = {
        let dir = TempDir::new().unwrap();
        let mut m = open(&dir).await;
        m.apply(history()).await.unwrap();
        contents(&mut m).await
    };

    let h = history();
    let orders: Vec<Vec<Vec<ChangeEvent>>> = vec![
        // Newest first, one event per batch.
        h.iter().rev().map(|e| vec![e.clone()]).collect(),
        // Two batches with overlap and in-batch duplicates.
        vec![
            vec![h[3].clone(), h[3].clone(), h[0].clone(), h[5].clone()],
            vec![h[1].clone(), h[0].clone(), h[2].clone(), h[4].clone(), h[3].clone()],
        ],
        // The delete first, then everything again.
        vec![vec![h[3].clone()], h.clone(), h.clone()],
    ];
    for (i, batches) in orders.into_iter().enumerate() {
        let dir = TempDir::new().unwrap();
        let mut m = open(&dir).await;
        for b in batches {
            m.apply(b).await.unwrap();
        }
        assert_eq!(contents(&mut m).await, expected, "order {i}");
    }
}

#[tokio::test]
async fn older_replay_does_not_win_scd1() {
    let dir = TempDir::new().unwrap();
    let mut m = open(&dir).await;
    let h = history();
    m.apply(vec![h[3].clone()]).await.unwrap(); // the delete
    let stats = m.apply(vec![h[0].clone()]).await.unwrap(); // the older create
    assert_eq!((stats.scd1_inserted, stats.scd1_updated), (0, 0));
    assert_eq!(stats.scd2_inserted, 1);
    // The delete stays current and unchanged; only the create is new.
    assert_eq!(stats.scd2_updated, 0);
    let (scd1, scd2) = contents(&mut m).await;
    assert_eq!(scd1.len(), 1);
    assert!(scd1[0].is_deleted);
    let v = versions(&scd2, "clicks", &json!({"id": 1}).to_string());
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].valid_to_seq.as_deref(), Some(seq(1, 5).as_str()));
    assert!(!v[0].is_current && v[1].is_current);
}

#[tokio::test]
async fn read_scd2_filters_by_key() {
    let dir = TempDir::new().unwrap();
    let mut m = open(&dir).await;
    m.apply(history()).await.unwrap();
    let key = ("Counter".to_string(), "clicks".to_string(), SCOPE.to_string(), json!({"id": 1}).to_string());
    let rows = m.read_scd2(Some(&BTreeSet::from([key.clone()]))).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|r| r.key() == key));
}

#[tokio::test]
async fn apply_change_files_matches_apply() {
    let h = history();
    let files = vec![
        celld_cdc_parquet::write_events(&h[..3]).unwrap(),
        celld_cdc_parquet::write_events(&[]).unwrap(),
        celld_cdc_parquet::write_events(&h[2..]).unwrap(),
    ];
    let dir = TempDir::new().unwrap();
    let mut m = open(&dir).await;
    let stats = m.apply_change_files(&files).await.unwrap();
    assert_eq!((stats.events, stats.unique_events, stats.keys), (7, 6, 4));

    let dir2 = TempDir::new().unwrap();
    let mut m2 = open(&dir2).await;
    m2.apply(h).await.unwrap();
    assert_eq!(contents(&mut m).await, contents(&mut m2).await);

    assert!(m.apply_change_files(&[bytes::Bytes::from_static(b"not parquet")]).await.is_err());
}
