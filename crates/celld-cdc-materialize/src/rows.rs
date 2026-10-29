//! The SCD1/SCD2 row models, their column lists, and Arrow conversion.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use deltalake::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, StringArray,
};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::{DataType, Field, Int64Type, Schema};
use deltalake::kernel::{DataType as DeltaType, StructField};
use serde::Serialize;

/// A column's type in both Arrow and Delta.
#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Str,
    I64,
    Bool,
}

/// `(name, kind, nullable)`.
pub(crate) type Col = (&'static str, Kind, bool);

/// The partition columns of both tables.
pub const PARTITION_COLUMNS: [&str; 2] = ["class", "table_name"];

/// SCD1 columns. `row` is the latest `after` as JSON, null once deleted.
pub(crate) const SCD1_COLS: &[Col] = &[
    ("scope", Kind::Str, false),
    ("class", Kind::Str, false),
    ("actor_name", Kind::Str, true),
    ("table_name", Kind::Str, false),
    ("pk", Kind::Str, false),
    ("row", Kind::Str, true),
    ("is_deleted", Kind::Bool, false),
    ("op", Kind::Str, false),
    ("epoch", Kind::I64, false),
    ("txid", Kind::I64, false),
    ("seq", Kind::Str, false),
    ("commit_ts_ms", Kind::I64, false),
    ("event_id", Kind::Str, false),
];

/// The SCD1 merge key.
pub(crate) const SCD1_KEY: &[&str] = &["class", "table_name", "scope", "pk"];

/// SCD2 columns. One row per event (version); `row` is null for a delete
/// tombstone.
pub(crate) const SCD2_COLS: &[Col] = &[
    ("scope", Kind::Str, false),
    ("class", Kind::Str, false),
    ("actor_name", Kind::Str, true),
    ("table_name", Kind::Str, false),
    ("pk", Kind::Str, false),
    ("row", Kind::Str, true),
    ("op", Kind::Str, false),
    ("is_deleted", Kind::Bool, false),
    ("epoch", Kind::I64, false),
    ("txid", Kind::I64, false),
    ("valid_from_seq", Kind::Str, false),
    ("valid_to_seq", Kind::Str, true),
    ("valid_from_ts_ms", Kind::I64, false),
    ("valid_to_ts_ms", Kind::I64, true),
    ("is_current", Kind::Bool, false),
    ("event_id", Kind::Str, false),
];

/// The SCD2 merge key: one version per key and position.
pub(crate) const SCD2_KEY: &[&str] = &["class", "table_name", "scope", "pk", "valid_from_seq"];

pub(crate) fn arrow_schema(cols: &[Col]) -> Arc<Schema> {
    Arc::new(Schema::new(
        cols.iter()
            .map(|&(n, k, null)| {
                let t = match k {
                    Kind::Str => DataType::Utf8,
                    Kind::I64 => DataType::Int64,
                    Kind::Bool => DataType::Boolean,
                };
                Field::new(n, t, null)
            })
            .collect::<Vec<_>>(),
    ))
}

pub(crate) fn delta_fields(cols: &[Col]) -> Vec<StructField> {
    cols.iter()
        .map(|&(n, k, null)| {
            let t = match k {
                Kind::Str => DeltaType::STRING,
                Kind::I64 => DeltaType::LONG,
                Kind::Bool => DeltaType::BOOLEAN,
            };
            StructField::new(n, t, null)
        })
        .collect()
}

/// The identity of a source row: `(class, table_name, scope, pk)`.
pub type RowKey = (String, String, String, String);

/// One SCD1 row: the latest state of one source row.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Scd1Row {
    pub scope: String,
    pub class: String,
    pub actor_name: Option<String>,
    pub table_name: String,
    pub pk: String,
    pub row: Option<String>,
    pub is_deleted: bool,
    pub op: String,
    pub epoch: i64,
    pub txid: i64,
    pub seq: String,
    pub commit_ts_ms: i64,
    pub event_id: String,
}

/// One SCD2 row: one version of one source row.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Scd2Row {
    pub scope: String,
    pub class: String,
    pub actor_name: Option<String>,
    pub table_name: String,
    pub pk: String,
    pub row: Option<String>,
    pub op: String,
    pub is_deleted: bool,
    pub epoch: i64,
    pub txid: i64,
    pub valid_from_seq: String,
    pub valid_to_seq: Option<String>,
    pub valid_from_ts_ms: i64,
    pub valid_to_ts_ms: Option<i64>,
    pub is_current: bool,
    pub event_id: String,
}

impl Scd2Row {
    pub fn key(&self) -> RowKey {
        (self.class.clone(), self.table_name.clone(), self.scope.clone(), self.pk.clone())
    }
}

/// Builds a batch from named columns, in `cols` order.
fn batch(cols: &[Col], mut arrays: HashMap<&'static str, ArrayRef>) -> Result<RecordBatch> {
    let columns = cols
        .iter()
        .map(|(n, _, _)| arrays.remove(n).with_context(|| format!("no array for {n}")))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(arrow_schema(cols), columns)?)
}

fn strs<T>(rows: &[T], f: impl Fn(&T) -> Option<&str>) -> ArrayRef {
    Arc::new(rows.iter().map(f).collect::<StringArray>())
}

fn ints<T>(rows: &[T], f: impl Fn(&T) -> Option<i64>) -> ArrayRef {
    Arc::new(rows.iter().map(f).collect::<Int64Array>())
}

fn bools<T>(rows: &[T], f: impl Fn(&T) -> bool) -> ArrayRef {
    Arc::new(rows.iter().map(|r| Some(f(r))).collect::<BooleanArray>())
}

pub(crate) fn scd1_batch(rows: &[Scd1Row]) -> Result<RecordBatch> {
    let a = HashMap::from([
        ("scope", strs(rows, |r| Some(&r.scope))),
        ("class", strs(rows, |r| Some(&r.class))),
        ("actor_name", strs(rows, |r| r.actor_name.as_deref())),
        ("table_name", strs(rows, |r| Some(&r.table_name))),
        ("pk", strs(rows, |r| Some(&r.pk))),
        ("row", strs(rows, |r| r.row.as_deref())),
        ("is_deleted", bools(rows, |r| r.is_deleted)),
        ("op", strs(rows, |r| Some(&r.op))),
        ("epoch", ints(rows, |r| Some(r.epoch))),
        ("txid", ints(rows, |r| Some(r.txid))),
        ("seq", strs(rows, |r| Some(&r.seq))),
        ("commit_ts_ms", ints(rows, |r| Some(r.commit_ts_ms))),
        ("event_id", strs(rows, |r| Some(&r.event_id))),
    ]);
    batch(SCD1_COLS, a)
}

pub(crate) fn scd2_batch(rows: &[Scd2Row]) -> Result<RecordBatch> {
    let a = HashMap::from([
        ("scope", strs(rows, |r| Some(&r.scope))),
        ("class", strs(rows, |r| Some(&r.class))),
        ("actor_name", strs(rows, |r| r.actor_name.as_deref())),
        ("table_name", strs(rows, |r| Some(&r.table_name))),
        ("pk", strs(rows, |r| Some(&r.pk))),
        ("row", strs(rows, |r| r.row.as_deref())),
        ("op", strs(rows, |r| Some(&r.op))),
        ("is_deleted", bools(rows, |r| r.is_deleted)),
        ("epoch", ints(rows, |r| Some(r.epoch))),
        ("txid", ints(rows, |r| Some(r.txid))),
        ("valid_from_seq", strs(rows, |r| Some(&r.valid_from_seq))),
        ("valid_to_seq", strs(rows, |r| r.valid_to_seq.as_deref())),
        ("valid_from_ts_ms", ints(rows, |r| Some(r.valid_from_ts_ms))),
        ("valid_to_ts_ms", ints(rows, |r| r.valid_to_ts_ms)),
        ("is_current", bools(rows, |r| r.is_current)),
        ("event_id", strs(rows, |r| Some(&r.event_id))),
    ]);
    batch(SCD2_COLS, a)
}

/// Typed, cast-normalized access to a scanned batch. Scans may return
/// `Utf8View` or dictionary-encoded partition columns; everything is cast
/// to the plain types first.
struct Cols<'a>(&'a RecordBatch);

impl Cols<'_> {
    fn get(&self, name: &str, t: &DataType) -> Result<ArrayRef> {
        let c = self.0.column_by_name(name).with_context(|| format!("missing column {name}"))?;
        Ok(if c.data_type() == t { c.clone() } else { cast(c, t)? })
    }
    fn s(&self, name: &str) -> Result<StringArray> {
        Ok(self.get(name, &DataType::Utf8)?.as_string::<i32>().clone())
    }
    fn i(&self, name: &str) -> Result<Int64Array> {
        Ok(self.get(name, &DataType::Int64)?.as_primitive::<Int64Type>().clone())
    }
    fn b(&self, name: &str) -> Result<BooleanArray> {
        Ok(self.get(name, &DataType::Boolean)?.as_boolean().clone())
    }
}

fn os(a: &StringArray, i: usize) -> Option<String> {
    (!a.is_null(i)).then(|| a.value(i).to_string())
}

fn oi(a: &Int64Array, i: usize) -> Option<i64> {
    (!a.is_null(i)).then(|| a.value(i))
}

pub(crate) fn scd1_from_batch(b: &RecordBatch) -> Result<Vec<Scd1Row>> {
    let c = Cols(b);
    let (scope, class, actor, table, pk, row, op, seq, eid) = (
        c.s("scope")?,
        c.s("class")?,
        c.s("actor_name")?,
        c.s("table_name")?,
        c.s("pk")?,
        c.s("row")?,
        c.s("op")?,
        c.s("seq")?,
        c.s("event_id")?,
    );
    let (del, epoch, txid, ts) = (c.b("is_deleted")?, c.i("epoch")?, c.i("txid")?, c.i("commit_ts_ms")?);
    Ok((0..b.num_rows())
        .map(|i| Scd1Row {
            scope: scope.value(i).into(),
            class: class.value(i).into(),
            actor_name: os(&actor, i),
            table_name: table.value(i).into(),
            pk: pk.value(i).into(),
            row: os(&row, i),
            is_deleted: del.value(i),
            op: op.value(i).into(),
            epoch: epoch.value(i),
            txid: txid.value(i),
            seq: seq.value(i).into(),
            commit_ts_ms: ts.value(i),
            event_id: eid.value(i).into(),
        })
        .collect())
}

pub(crate) fn scd2_from_batch(b: &RecordBatch) -> Result<Vec<Scd2Row>> {
    let c = Cols(b);
    let (scope, class, actor, table, pk, row, op, from, to, eid) = (
        c.s("scope")?,
        c.s("class")?,
        c.s("actor_name")?,
        c.s("table_name")?,
        c.s("pk")?,
        c.s("row")?,
        c.s("op")?,
        c.s("valid_from_seq")?,
        c.s("valid_to_seq")?,
        c.s("event_id")?,
    );
    let (del, cur, epoch, txid, from_ts, to_ts) = (
        c.b("is_deleted")?,
        c.b("is_current")?,
        c.i("epoch")?,
        c.i("txid")?,
        c.i("valid_from_ts_ms")?,
        c.i("valid_to_ts_ms")?,
    );
    Ok((0..b.num_rows())
        .map(|i| Scd2Row {
            scope: scope.value(i).into(),
            class: class.value(i).into(),
            actor_name: os(&actor, i),
            table_name: table.value(i).into(),
            pk: pk.value(i).into(),
            row: os(&row, i),
            op: op.value(i).into(),
            is_deleted: del.value(i),
            epoch: epoch.value(i),
            txid: txid.value(i),
            valid_from_seq: from.value(i).into(),
            valid_to_seq: os(&to, i),
            valid_from_ts_ms: from_ts.value(i),
            valid_to_ts_ms: oi(&to_ts, i),
            is_current: cur.value(i),
            event_id: eid.value(i).into(),
        })
        .collect())
}
