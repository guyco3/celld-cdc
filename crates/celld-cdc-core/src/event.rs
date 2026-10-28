//! The change-event contract shared by the decoder, the Parquet change log,
//! and the SCD materializer.
//!
//! One event is one row-level change in one table of one actor, produced by
//! diffing the actor's database before and after one LTX segment. A segment
//! can hold several SQLite commits, so an event carries the net change of
//! its segment, never an intermediate state inside it.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// The operation, in Debezium's single-letter vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Op {
    /// Create: the key is absent before the segment and present after.
    #[serde(rename = "c")]
    Create,
    /// Update: the key is present on both sides with a different row.
    #[serde(rename = "u")]
    Update,
    /// Delete: the key is present before the segment and absent after.
    #[serde(rename = "d")]
    Delete,
    /// Read: a row seen in a snapshot the decoder started from, with no
    /// earlier state to diff against (Debezium's snapshot read).
    #[serde(rename = "r")]
    Read,
}

impl Op {
    pub fn as_str(self) -> &'static str {
        match self {
            Op::Create => "c",
            Op::Update => "u",
            Op::Delete => "d",
            Op::Read => "r",
        }
    }

    pub fn parse(s: &str) -> Option<Op> {
        Some(match s {
            "c" => Op::Create,
            "u" => Op::Update,
            "d" => Op::Delete,
            "r" => Op::Read,
            _ => return None,
        })
    }
}

/// One row-level change.
///
/// `pk`, `before`, and `after` are JSON objects keyed by column name. SQLite
/// is dynamically typed, so a column's values keep their stored type:
/// integers and reals are JSON numbers, text is a string, NULL is null, and
/// a BLOB is `{"$b64": "<base64>"}`. A `_cf_KV` value is decoded from the V8
/// serialization into plain JSON where it can be, and is otherwise
/// `{"$v8": "<base64>"}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Deterministic: hex sha256 of `scope|epoch|txid|table|pk`. A replayed
    /// segment produces the same ids, so every sink dedupes on it.
    pub event_id: String,
    /// The Durable Object class, the part of the scope before the first `:`.
    pub class: String,
    /// The celld cell scope, `{Class}:{64-hex id}`.
    pub scope: String,
    /// The `idFromName` name from `_cf_METADATA`, when the actor has one.
    pub actor_name: Option<String>,
    /// The SQLite table name.
    pub table: String,
    pub op: Op,
    /// The epoch of the segment that produced the change.
    pub epoch: u64,
    /// The segment's first txid.
    pub min_txid: u64,
    /// The segment's last txid: the position the change is visible at.
    pub txid: u64,
    /// The total order of an actor's changes: [`seq`] of `(epoch, txid)`.
    pub seq: String,
    /// The segment's LTX header timestamp, in Unix milliseconds.
    pub commit_ts_ms: i64,
    /// The bucket key the segment was read from (an `.ltx` or `.ltxb`).
    pub source_key: String,
    pub pk: Value,
    pub before: Option<Value>,
    pub after: Option<Value>,
}

/// The sortable position string of `(epoch, txid)`. Epochs only grow, and a
/// txid only grows within an epoch, so string order is the actor's order.
pub fn seq(epoch: u64, txid: u64) -> String {
    format!("e{epoch:016x}-t{txid:016x}")
}

/// The deterministic event id. `pk` is serialized with serde_json, whose
/// object keys are sorted (no `preserve_order` feature), so equal keys hash
/// equally.
pub fn event_id(scope: &str, epoch: u64, txid: u64, table: &str, pk: &Value) -> String {
    let mut h = Sha256::new();
    h.update(scope.as_bytes());
    h.update(b"|");
    h.update(epoch.to_string().as_bytes());
    h.update(b"|");
    h.update(txid.to_string().as_bytes());
    h.update(b"|");
    h.update(table.as_bytes());
    h.update(b"|");
    h.update(pk.to_string().as_bytes());
    hex::encode(h.finalize())
}

/// The class part of a scope: everything before the first `:`.
pub fn class_of(scope: &str) -> &str {
    scope.split_once(':').map_or(scope, |(class, _)| class)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn seq_orders_by_epoch_then_txid() {
        assert!(seq(1, 0xffff) < seq(2, 1));
        assert!(seq(2, 9) < seq(2, 10));
    }

    #[test]
    fn event_id_ignores_key_order() {
        let a = event_id("C:1", 1, 2, "t", &json!({"a": 1, "b": 2}));
        let b = event_id("C:1", 1, 2, "t", &json!({"b": 2, "a": 1}));
        assert_eq!(a, b);
        assert_ne!(a, event_id("C:1", 1, 3, "t", &json!({"a": 1, "b": 2})));
    }

    #[test]
    fn op_round_trips_through_json() {
        let s = serde_json::to_string(&Op::Delete).unwrap();
        assert_eq!(s, "\"d\"");
        assert_eq!(serde_json::from_str::<Op>(&s).unwrap(), Op::Delete);
    }
}
