//! Row-level diff of two database images.
//!
//! [`full_diff`] reads every captured table on both sides and compares rows
//! by key. It is simple and exact, and is the oracle any faster diff must
//! agree with.
//!
//! Which tables are captured: every user table, plus `_cf_KV` (the
//! `ctx.storage` key-value API). celld's other internal tables
//! (`_cf_ALARM`, `_cf_WAKE`, `_cf_FACETS`, `_cf_METADATA`), litestream's,
//! SQLite's, and virtual tables are skipped.
//!
//! A row's key is its declared PRIMARY KEY columns; a table without one is
//! keyed by `{"_rowid": n}`. `_cf_KV` is keyed by `{"k": key}` and its row
//! is `{"k": key, "v": <decoded value>}`; the scope column is dropped, since
//! it always names the actor itself.

use crate::v8value::decode_kv_value;
use anyhow::{Context, Result};
use base64::Engine;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::Path;

pub const KV_TABLE: &str = "_cf_KV";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowChange {
    pub table: String,
    pub kind: Kind,
    pub pk: Value,
    pub before: Option<Value>,
    pub after: Option<Value>,
}

/// Open an image read-only. An empty file is a database with no tables.
pub fn open(path: &Path) -> Result<Connection> {
    let uri = format!("file:{}?immutable=1", path.display());
    let conn = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open {}", path.display()))?;
    Ok(conn)
}

/// The changes that turn `before` into `after`, ordered by table then key.
pub fn full_diff(before: &Connection, after: &Connection) -> Result<Vec<RowChange>> {
    let mut tables = captured_tables(before)?;
    for t in captured_tables(after)? {
        if !tables.contains(&t) {
            tables.push(t);
        }
    }
    tables.sort();
    let mut out = Vec::new();
    for table in tables {
        compare(&table, read_rows(before, &table, None)?, read_rows(after, &table, None)?, &mut out);
    }
    Ok(out)
}

/// Append the changes between two sets of one table's rows.
pub fn compare(table: &str, old: Rows, new: Rows, out: &mut Vec<RowChange>) {
    for (k, (pk, row)) in &new {
        match old.get(k) {
            None => out.push(RowChange {
                table: table.to_string(),
                kind: Kind::Insert,
                pk: pk.clone(),
                before: None,
                after: Some(row.clone()),
            }),
            Some((_, prev)) if prev != row => out.push(RowChange {
                table: table.to_string(),
                kind: Kind::Update,
                pk: pk.clone(),
                before: Some(prev.clone()),
                after: Some(row.clone()),
            }),
            Some(_) => {}
        }
    }
    for (k, (pk, row)) in old {
        if !new.contains_key(&k) {
            out.push(RowChange {
                table: table.to_string(),
                kind: Kind::Delete,
                pk,
                before: Some(row),
                after: None,
            });
        }
    }
}

/// The actor's `idFromName` name, if it has one.
pub fn actor_name(conn: &Connection, scope: &str) -> Result<Option<String>> {
    if !has_table(conn, "_cf_METADATA")? {
        return Ok(None);
    }
    let name = conn
        .query_row(
            "SELECT actor_name FROM _cf_METADATA WHERE scope = ?1",
            [scope],
            |r| r.get::<_, Option<String>>(0),
        )
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            e => Err(e),
        })?;
    Ok(name)
}

pub(crate) fn has_table(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [name],
            |_| Ok(()),
        )
        .is_ok())
}

/// Whether `name` is a table this crate emits changes for.
pub fn is_captured(name: &str, sql: &str) -> bool {
    if name == KV_TABLE {
        return true;
    }
    let internal = ["sqlite_", "_cf_", "_litestream_"];
    !internal.iter().any(|p| name.starts_with(p))
        && !sql.trim_start().to_ascii_uppercase().starts_with("CREATE VIRTUAL")
}

pub(crate) fn captured_tables(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT name, coalesce(sql, '') FROM sqlite_schema WHERE type = 'table'")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    let mut tables = Vec::new();
    let mut virtuals = Vec::new();
    for row in rows {
        let (name, sql) = row?;
        if sql.trim_start().to_ascii_uppercase().starts_with("CREATE VIRTUAL") {
            virtuals.push(name.clone());
        }
        if is_captured(&name, &sql) {
            tables.push(name);
        }
    }
    // A virtual table's shadow tables (`{name}_data`, ...) are its storage.
    tables.retain(|t| !virtuals.iter().any(|v| t.starts_with(&format!("{v}_"))));
    Ok(tables)
}

/// One table's rows keyed by the serialized key: `(key, row)`.
pub type Rows = BTreeMap<String, (Value, Value)>;

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The rows of `table` keyed by the serialized key, or none if the table
/// does not exist on this side. `rowids` limits the read to those rows.
pub fn read_rows(conn: &Connection, table: &str, rowids: Option<&[i64]>) -> Result<Rows> {
    if !has_table(conn, table)? {
        return Ok(Rows::new());
    }
    if let Some(ids) = rowids {
        let mut rows = Rows::new();
        for chunk in ids.chunks(500) {
            let list: Vec<String> = chunk.iter().map(i64::to_string).collect();
            rows.append(&mut read_rows_where(conn, table, &format!(" WHERE rowid IN ({})", list.join(",")))?);
        }
        return Ok(rows);
    }
    read_rows_where(conn, table, "")
}

fn read_rows_where(conn: &Connection, table: &str, filter: &str) -> Result<Rows> {
    if table == KV_TABLE {
        return read_kv(conn, filter);
    }
    let mut pk_cols: Vec<(i64, String)> = Vec::new();
    {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote(table)))?;
        let cols = stmt.query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, i64>(5)?)))?;
        for col in cols {
            let (name, pk) = col?;
            if pk > 0 {
                pk_cols.push((pk, name));
            }
        }
    }
    pk_cols.sort();
    let without_rowid: bool = conn.query_row(
        "SELECT coalesce(sql, '') LIKE '%WITHOUT ROWID%' FROM sqlite_schema WHERE name = ?1",
        [table],
        |r| r.get(0),
    )?;
    let by_rowid = pk_cols.is_empty() && !without_rowid;
    let select = if by_rowid {
        format!("SELECT rowid AS \"__cdc_rowid\", * FROM {}{filter}", quote(table))
    } else {
        format!("SELECT * FROM {}{filter}", quote(table))
    };
    let mut stmt = conn.prepare(&select)?;
    let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut rows = Rows::new();
    let mut q = stmt.query([])?;
    while let Some(r) = q.next()? {
        let mut row = Map::new();
        let mut rowid = None;
        for (i, name) in names.iter().enumerate() {
            let v = to_json(r.get_ref(i)?);
            if by_rowid && i == 0 {
                rowid = Some(v);
            } else {
                row.insert(name.clone(), v);
            }
        }
        let pk = match rowid {
            Some(id) => json!({ "_rowid": id }),
            None => Value::Object(
                pk_cols
                    .iter()
                    .map(|(_, c)| (c.clone(), row.get(c).cloned().unwrap_or(Value::Null)))
                    .collect(),
            ),
        };
        rows.insert(pk.to_string(), (pk, Value::Object(row)));
    }
    Ok(rows)
}

fn read_kv(conn: &Connection, filter: &str) -> Result<Rows> {
    let mut stmt = conn.prepare(&format!("SELECT k, v FROM {}{filter}", quote(KV_TABLE)))?;
    let mut rows = Rows::new();
    let mut q = stmt.query([])?;
    while let Some(r) = q.next()? {
        let k = to_json(r.get_ref(0)?);
        let v = match r.get_ref(1)? {
            ValueRef::Blob(b) | ValueRef::Text(b) => decode_kv_value(b),
            other => to_json(other),
        };
        let pk = json!({ "k": k });
        rows.insert(pk.to_string(), (pk, json!({ "k": k, "v": v })));
    }
    Ok(rows)
}

/// A stored SQLite value as JSON: numbers, strings, null, and BLOBs as
/// `{"$b64": ...}`. A non-finite real becomes `{"$double": "Infinity"}`.
pub fn to_json(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => json!(i),
        ValueRef::Real(f) if f.is_finite() => json!(f),
        ValueRef::Real(f) => json!({ "$double": f.to_string() }),
        ValueRef::Text(t) => Value::String(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => json!({ "$b64": base64::engine::general_purpose::STANDARD.encode(b) }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(sql: &str) -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        Connection::open(&path).unwrap().execute_batch(sql).unwrap();
        let conn = open(&path).unwrap();
        (dir, conn)
    }

    #[test]
    fn diffs_inserts_updates_and_deletes_by_primary_key() {
        let schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT);
                      CREATE TABLE _cf_ALARM(scope TEXT PRIMARY KEY, at_ms INTEGER);";
        let (_a, before) = db(&format!(
            "{schema} INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'z'); INSERT INTO _cf_ALARM VALUES ('s',1);"
        ));
        let (_b, after) = db(&format!(
            "{schema} INSERT INTO t VALUES (1,'x'),(2,'Y'),(4,'w'); INSERT INTO _cf_ALARM VALUES ('s',2);"
        ));
        let changes = full_diff(&before, &after).unwrap();
        let summary: Vec<_> = changes.iter().map(|c| (c.kind, c.pk.to_string())).collect();
        assert_eq!(
            summary,
            vec![
                (Kind::Update, r#"{"id":2}"#.to_string()),
                (Kind::Insert, r#"{"id":4}"#.to_string()),
                (Kind::Delete, r#"{"id":3}"#.to_string()),
            ]
        );
        assert_eq!(changes[0].before, Some(json!({"id": 2, "a": "y"})));
    }

    #[test]
    fn tables_without_a_key_use_the_rowid_and_blobs_are_base64() {
        let (_a, before) = db("CREATE TABLE log(msg BLOB);");
        let (_b, after) = db("CREATE TABLE log(msg BLOB); INSERT INTO log VALUES (x'0102');");
        let changes = full_diff(&before, &after).unwrap();
        assert_eq!(changes[0].pk, json!({"_rowid": 1}));
        assert_eq!(changes[0].after, Some(json!({"msg": {"$b64": "AQI="}})));
    }

    #[test]
    fn a_new_table_is_all_inserts_and_an_empty_file_has_no_tables() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.db");
        std::fs::File::create(&empty).unwrap();
        let (_b, after) = db("CREATE TABLE t(k TEXT PRIMARY KEY, v) WITHOUT ROWID; INSERT INTO t VALUES ('a', 1);");
        let changes = full_diff(&open(&empty).unwrap(), &after).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].pk, json!({"k": "a"}));
    }
}
