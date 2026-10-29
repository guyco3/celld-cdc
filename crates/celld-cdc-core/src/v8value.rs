//! Decode a `_cf_KV.v` value into JSON.
//!
//! celld stores `ctx.storage.put` values as V8 ValueSerializer payloads
//! (header `ff 0f`, the wire version pinned to 15 like workerd). Older rows
//! hold JSON text instead. The decoded form is plain JSON where JSON has a
//! natural equivalent, and a single-key `$`-tagged object where it does not:
//!
//! - `undefined` and array holes are `null`; integral doubles within ±2^53
//!   are JSON integers; NaN and ±Infinity are `{"$double": "NaN" |
//!   "Infinity" | "-Infinity"}`.
//! - A BigInt is `{"$bigint": "<decimal>"}`; a Date is `{"$date": <ms>}`;
//!   a RegExp is `{"$regexp": {"source": .., "flags": ..}}`; an Error is
//!   `{"$error": {"name", "message"?, "stack"?, "cause"?}}`.
//! - A Map is `{"$map": [[k, v], ..]}`; a Set is `{"$set": [..]}`.
//! - An ArrayBuffer or typed-array/DataView is `{"$bytes": "<base64>"}` of
//!   the bytes it covers.
//! - Primitive wrappers (`new String("x")` etc.) are their primitive value.
//! - Numeric property keys become their string form; non-index properties
//!   on arrays are dropped. A repeated object is repeated in full.
//! - A stored-stub row (`0x01` + clone of a marker tree) is
//!   `{"$stubs": <tree>}`, its `__celld$do` / `__celld$svc` markers left as
//!   plain objects.
//!
//! Anything else (cyclic references, host objects, shared or transferred
//! buffers, WebAssembly, unknown tags, malformed bytes) makes the whole value
//! `{"$v8": "<base64 of the original bytes>"}`.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::{json, Map, Number, Value};

/// First byte of a stored-stub row (celld `storage_ops.rs`).
const STORED_STUB_TAG: u8 = 0x01;
const MAX_DEPTH: usize = 256;
const MAX_NODES: usize = 1 << 20;
const MAX_SPARSE_LEN: usize = 1 << 20;

/// Decode a `_cf_KV.v` value: a BLOB's bytes, or a legacy TEXT value's UTF-8
/// bytes. Never fails; see the module docs for the output form.
pub fn decode_kv_value(bytes: &[u8]) -> Value {
    let decoded = match bytes.first() {
        Some(0xff) => decode_v8(bytes),
        Some(&STORED_STUB_TAG) => decode_v8(&bytes[1..]).map(|tree| json!({ "$stubs": tree })),
        _ => serde_json::from_slice(bytes).ok(),
    };
    decoded.unwrap_or_else(|| json!({ "$v8": STANDARD.encode(bytes) }))
}

fn decode_v8(bytes: &[u8]) -> Option<Value> {
    let mut d = Decoder {
        buf: bytes,
        pos: 0,
        version: 0,
        ids: Vec::new(),
        buffers: Vec::new(),
        depth: 0,
        nodes: 0,
    };
    if d.byte()? != 0xff {
        return None;
    }
    d.version = d.varint32()?;
    if !(13..=16).contains(&d.version) {
        return None;
    }
    d.value()
}

/// A slot per id-bearing object: `None` while it is still being read.
type Slot = Option<(Value, usize)>;

struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
    version: u32,
    ids: Vec<Slot>,
    buffers: Vec<(usize, &'a [u8])>,
    depth: usize,
    nodes: usize,
}

impl<'a> Decoder<'a> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }

    fn varint(&mut self) -> Option<u64> {
        let mut out = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            out |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Some(out);
            }
        }
        None
    }

    fn varint32(&mut self) -> Option<u32> {
        u32::try_from(self.varint()?).ok()
    }

    fn len(&mut self) -> Option<usize> {
        usize::try_from(self.varint32()?).ok()
    }

    fn f64(&mut self) -> Option<f64> {
        Some(f64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    /// Next tag, skipping padding.
    fn tag(&mut self) -> Option<u8> {
        loop {
            match self.byte()? {
                0x00 => continue,
                t => return Some(t),
            }
        }
    }

    fn peek_tag(&mut self) -> Option<u8> {
        let start = self.pos;
        let t = self.tag();
        self.pos = start;
        t
    }

    /// Consume `end` if it is the next tag.
    fn end(&mut self, end: u8) -> Option<bool> {
        if self.peek_tag()? == end {
            self.tag();
            Some(true)
        } else {
            Some(false)
        }
    }

    fn value(&mut self) -> Option<Value> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return None;
        }
        let v = self.value_inner()?;
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return None;
        }
        self.depth -= 1;
        Some(v)
    }

    fn value_inner(&mut self) -> Option<Value> {
        let mut tag = self.tag()?;
        while tag == b'?' {
            self.varint()?;
            tag = self.tag()?;
        }
        let v = match tag {
            b'_' | b'0' => Value::Null,
            b'T' => Value::Bool(true),
            b'F' => Value::Bool(false),
            b'I' => {
                let n = self.varint32()?;
                Value::from(((n >> 1) as i32) ^ -((n & 1) as i32))
            }
            b'U' => Value::from(self.varint32()?),
            b'N' => number(self.f64()?),
            b'Z' => self.bigint()?,
            b'"' | b'c' | b'S' => Value::String(self.string_body(tag)?),
            b'^' => {
                let id = self.len()?;
                if let Some(&(_, buffer)) = self.buffers.iter().find(|(b, _)| *b == id) {
                    if self.peek_tag() == Some(b'V') {
                        return self.view(buffer);
                    }
                }
                let (v, size) = self.ids.get(id)?.clone()?;
                self.nodes = self.nodes.checked_add(size)?;
                return Some(v);
            }
            b'B' | b'~' => {
                let id = self.reserve();
                let n = self.len()?;
                if tag == b'~' {
                    self.varint32()?;
                }
                let buffer = self.take(n)?;
                self.buffers.push((id, buffer));
                self.ids[id] = Some((bytes_value(buffer), 1));
                if self.peek_tag() == Some(b'V') {
                    return self.view(buffer);
                }
                bytes_value(buffer)
            }
            _ => {
                let id = self.reserve();
                let start = self.nodes;
                let v = self.object(tag)?;
                let size = self.nodes - start;
                self.ids[id] = Some((v.clone(), size));
                return Some(v);
            }
        };
        Some(v)
    }

    fn reserve(&mut self) -> usize {
        self.ids.push(None);
        self.ids.len() - 1
    }

    /// The typed array or DataView that follows its buffer.
    fn view(&mut self, buffer: &[u8]) -> Option<Value> {
        self.tag();
        let id = self.reserve();
        self.varint32()?;
        let offset = self.len()?;
        let n = self.len()?;
        if self.version >= 14 {
            self.varint32()?;
        }
        let v = bytes_value(buffer.get(offset..offset.checked_add(n)?)?);
        self.ids[id] = Some((v.clone(), 1));
        Some(v)
    }

    /// An id-bearing object other than a buffer.
    fn object(&mut self, tag: u8) -> Option<Value> {
        Some(match tag {
            b'o' => {
                let mut obj = Map::new();
                while !self.end(b'{')? {
                    let (k, v) = self.property()?;
                    obj.insert(k, v);
                }
                self.varint32()?;
                Value::Object(obj)
            }
            b'A' => {
                let len = self.len()?;
                if len > self.buf.len() - self.pos {
                    return None;
                }
                let mut arr = Vec::with_capacity(len);
                for _ in 0..len {
                    if self.peek_tag()? == b'-' {
                        self.tag();
                        arr.push(Value::Null);
                    } else {
                        arr.push(self.value()?);
                    }
                }
                while !self.end(b'$')? {
                    self.property()?;
                }
                self.varint32()?;
                self.varint32()?;
                Value::Array(arr)
            }
            b'a' => {
                let len = self.len()?;
                if len > MAX_SPARSE_LEN {
                    return None;
                }
                let mut arr = vec![Value::Null; len];
                while !self.end(b'@')? {
                    let (k, v) = self.property()?;
                    if let Some(slot) = k.parse::<usize>().ok().and_then(|i| arr.get_mut(i)) {
                        *slot = v;
                    }
                }
                self.varint32()?;
                self.varint32()?;
                Value::Array(arr)
            }
            b';' => {
                let mut entries = Vec::new();
                while !self.end(b':')? {
                    let k = self.value()?;
                    let v = self.value()?;
                    entries.push(Value::Array(vec![k, v]));
                }
                self.varint32()?;
                json!({ "$map": entries })
            }
            b'\'' => {
                let mut items = Vec::new();
                while !self.end(b',')? {
                    items.push(self.value()?);
                }
                self.varint32()?;
                json!({ "$set": items })
            }
            b'D' => json!({ "$date": number(self.f64()?) }),
            b'y' => Value::Bool(true),
            b'x' => Value::Bool(false),
            b'n' => number(self.f64()?),
            b'z' => self.bigint()?,
            b's' => {
                let t = self.tag()?;
                Value::String(self.string_body(t)?)
            }
            b'R' => {
                let t = self.tag()?;
                let source = self.string_body(t)?;
                let bits = self.varint32()?;
                let flags: String = [
                    (128, 'd'),
                    (1, 'g'),
                    (2, 'i'),
                    (4, 'm'),
                    (32, 's'),
                    (16, 'u'),
                    (256, 'v'),
                    (8, 'y'),
                ]
                .iter()
                .filter(|(bit, _)| bits & bit != 0)
                .map(|&(_, c)| c)
                .collect();
                json!({ "$regexp": { "source": source, "flags": flags } })
            }
            b'r' => self.error()?,
            _ => return None,
        })
    }

    fn property(&mut self) -> Option<(String, Value)> {
        let k = match self.value()? {
            Value::String(s) => s,
            Value::Number(n) => n.to_string(),
            _ => return None,
        };
        Some((k, self.value()?))
    }

    fn string_body(&mut self, tag: u8) -> Option<String> {
        let n = self.len()?;
        let bytes = self.take(n)?;
        match tag {
            b'"' => Some(bytes.iter().map(|&b| char::from(b)).collect()),
            b'S' => String::from_utf8(bytes.to_vec()).ok(),
            b'c' if n % 2 == 0 => {
                let units: Vec<u16> = bytes
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|&c| u16::from_le_bytes(c))
                    .collect();
                Some(String::from_utf16_lossy(&units))
            }
            _ => None,
        }
    }

    fn bigint(&mut self) -> Option<Value> {
        let bits = self.varint32()?;
        let n = usize::try_from(bits >> 1).ok()?;
        let digits = self.take(n)?;
        let mut s = decimal(digits);
        if bits & 1 == 1 && s != "0" {
            s.insert(0, '-');
        }
        Some(json!({ "$bigint": s }))
    }

    fn error(&mut self) -> Option<Value> {
        let mut obj = Map::new();
        let mut name = "Error";
        loop {
            match self.tag()? {
                b'E' => name = "EvalError",
                b'R' => name = "RangeError",
                b'F' => name = "ReferenceError",
                b'S' => name = "SyntaxError",
                b'T' => name = "TypeError",
                b'U' => name = "URIError",
                t @ (b'm' | b's') => {
                    let st = self.tag()?;
                    let key = if t == b'm' { "message" } else { "stack" };
                    obj.insert(key.into(), Value::String(self.string_body(st)?));
                }
                b'c' => {
                    let cause = self.value()?;
                    obj.insert("cause".into(), cause);
                }
                b'.' => break,
                _ => return None,
            }
        }
        obj.insert("name".into(), name.into());
        Some(json!({ "$error": obj }))
    }
}

fn bytes_value(bytes: &[u8]) -> Value {
    json!({ "$bytes": STANDARD.encode(bytes) })
}

fn number(f: f64) -> Value {
    if f.is_nan() {
        json!({ "$double": "NaN" })
    } else if f.is_infinite() {
        json!({ "$double": if f > 0.0 { "Infinity" } else { "-Infinity" } })
    } else if f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0 {
        Value::from(f as i64)
    } else {
        Number::from_f64(f).map_or(Value::Null, Value::Number)
    }
}

/// Little-endian magnitude bytes as a decimal string.
fn decimal(le: &[u8]) -> String {
    let mut limbs: Vec<u32> = le
        .chunks(4)
        .map(|c| {
            let mut w = [0u8; 4];
            w[..c.len()].copy_from_slice(c);
            u32::from_le_bytes(w)
        })
        .collect();
    let mut chunks = Vec::new();
    loop {
        while limbs.last() == Some(&0) {
            limbs.pop();
        }
        if limbs.is_empty() {
            break;
        }
        let mut rem = 0u64;
        for limb in limbs.iter_mut().rev() {
            let cur = (rem << 32) | u64::from(*limb);
            *limb = (cur / 1_000_000_000) as u32;
            rem = cur % 1_000_000_000;
        }
        chunks.push(rem as u32);
    }
    match chunks.split_last() {
        None => "0".into(),
        Some((first, rest)) => {
            let mut s = first.to_string();
            for c in rest.iter().rev() {
                s.push_str(&format!("{c:09}"));
            }
            s
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(bytes: &[u8]) -> Value {
        json!({ "$v8": STANDARD.encode(bytes) })
    }

    #[test]
    fn malformed_falls_back_to_raw() {
        for bytes in [
            &b""[..],
            &[0xff],
            &[0xff, 0x0f],
            &[0xff, 0x0f, b'I'],
            &[0xff, 0x0f, b'"', 0x05, b'a'],
            &[0xff, 0x0f, b'o', b'"', 0x01, b'a'],
            &[0xff, 0x0f, b'^', 0x00],
            &[0xff, 0x0f, b'A', 0xff, 0xff, 0xff, 0xff, 0x0f],
            &[0xff, 0x0f, b'a', 0xff, 0xff, 0xff, 0xff, 0x0f, b'@', 0, 0],
            &[0xff, 0x0f, b'c', 0x03, 0x61, 0x00, 0x62],
            &[0xff, 0x0f, b'\\', 0x01],
            &[0xff, 0x0f, b'N', 0x00, 0x00],
            &[0xff, 0x0f, 0xfe],
            &[0xff, 0x0f, b'?', 0x01, b'?', 0x01],
            &[0xff, 0x03, b'T'],
            &[0xff, 0x0f, b'I', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            &[0x01],
            &[0x01, 0x02, 0x03],
            &b"{not json"[..],
        ] {
            assert_eq!(decode_kv_value(bytes), raw(bytes), "{bytes:02x?}");
        }
    }

    #[test]
    fn deep_nesting_falls_back() {
        let mut bytes = vec![0xff, 0x0f];
        bytes.extend(std::iter::repeat_n([b'A', 0x01], 10_000).flatten());
        bytes.push(b'0');
        assert_eq!(decode_kv_value(&bytes), raw(&bytes));
    }

    #[test]
    fn cycle_falls_back() {
        // const o = {}; o.self = o;
        let bytes = [
            0xff, 0x0f, b'o', b'"', 4, b's', b'e', b'l', b'f', b'^', 0, b'{', 1,
        ];
        assert_eq!(decode_kv_value(&bytes), raw(&bytes));
    }

    #[test]
    fn legacy_json_text() {
        assert_eq!(decode_kv_value(br#"{"a":[1,"x"]}"#), json!({"a": [1, "x"]}));
        assert_eq!(decode_kv_value(b"42"), json!(42));
    }

    #[test]
    fn stored_stub_row() {
        let bytes = [0x01, 0xff, 0x0f, b'o', b'"', 1, b'n', b'I', 2, b'{', 1];
        assert_eq!(decode_kv_value(&bytes), json!({"$stubs": {"n": 1}}));
    }

    #[test]
    fn bigint_decimal() {
        assert_eq!(decimal(&[]), "0");
        assert_eq!(decimal(&[0; 8]), "0");
        assert_eq!(decimal(&u64::MAX.to_le_bytes()), u64::MAX.to_string());
        let mut b = u128::MAX.to_le_bytes().to_vec();
        b.extend([0; 8]);
        assert_eq!(decimal(&b), u128::MAX.to_string());
    }
}
