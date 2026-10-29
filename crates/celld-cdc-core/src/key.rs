//! Parse the celld bucket keys that celld-cdc consumes.
//!
//! - `{prefix}cells/{scope}/ltx/e{epoch}/{level:04x}/{min:016x}-{max:016x}.ltx`
//! - `{prefix}log/{node}/{generation}/bundle/e{log_epoch}-{seq:08}.ltxb`
//!
//! S3 event notifications URL-encode keys (a scope's `:` arrives as `%3A`
//! and a space as `+`), so [`decode_event_key`] undoes that first.

use percent_encoding::percent_decode_str;

/// A key this pipeline cares about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CelldKey {
    Ltx(LtxKey),
    Bundle(BundleKey),
}

/// One per-cell LTX object.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LtxKey {
    pub scope: String,
    pub epoch: u64,
    pub level: u32,
    pub min_txid: u64,
    pub max_txid: u64,
}

/// One node-log bundle object; it holds L0 segments of many cells.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BundleKey {
    pub node: String,
    pub generation: String,
    pub name: String,
}

/// Undo S3 notification key encoding: `+` is a space, then percent-decoding.
pub fn decode_event_key(raw: &str) -> String {
    percent_decode_str(&raw.replace('+', " "))
        .decode_utf8_lossy()
        .into_owned()
}

/// Parse a bucket key relative to the fleet `prefix` (`""` or `"x/"`).
pub fn parse(prefix: &str, key: &str) -> Option<CelldKey> {
    let rest = key.strip_prefix(prefix)?;
    if let Some(rest) = rest.strip_prefix("cells/") {
        return parse_ltx(rest).map(CelldKey::Ltx);
    }
    if let Some(rest) = rest.strip_prefix("log/") {
        return parse_bundle(rest).map(CelldKey::Bundle);
    }
    None
}

fn parse_ltx(rest: &str) -> Option<LtxKey> {
    // A facet stream nests under `{root}/facets/{hex}/ltx/...`; celld-cdc
    // follows root cells only.
    let (scope, rest) = rest.split_once("/ltx/")?;
    if scope.contains('/') {
        return None;
    }
    let mut parts = rest.split('/');
    let epoch = parts.next()?.strip_prefix('e')?.parse().ok()?;
    let level = u32::from_str_radix(parts.next()?, 16).ok()?;
    let name = parts.next()?.strip_suffix(".ltx")?;
    if parts.next().is_some() {
        return None;
    }
    let (min, max) = name.split_once('-')?;
    let min_txid = parse_txid(min)?;
    let max_txid = parse_txid(max)?;
    (min_txid <= max_txid).then(|| LtxKey {
        scope: scope.to_string(),
        epoch,
        level,
        min_txid,
        max_txid,
    })
}

fn parse_txid(s: &str) -> Option<u64> {
    (s.len() == 16).then(|| u64::from_str_radix(s, 16).ok())?
}

fn parse_bundle(rest: &str) -> Option<BundleKey> {
    let mut parts = rest.split('/');
    let node = parts.next()?;
    let generation = parts.next()?;
    if parts.next()? != "bundle" {
        return None;
    }
    let name = parts.next()?;
    if parts.next().is_some() || !name.ends_with(".ltxb") {
        return None;
    }
    Some(BundleKey {
        node: node.to_string(),
        generation: generation.to_string(),
        name: name.to_string(),
    })
}

impl LtxKey {
    pub fn to_key(&self, prefix: &str) -> String {
        format!(
            "{prefix}{}{:016x}-{:016x}.ltx",
            level_prefix(&self.scope, self.epoch, self.level),
            self.min_txid,
            self.max_txid
        )
    }
}

impl BundleKey {
    pub fn to_key(&self, prefix: &str) -> String {
        format!(
            "{prefix}log/{}/{}/bundle/{}",
            self.node, self.generation, self.name
        )
    }
}

/// `cells/{scope}/ltx/`, the parent of a cell's epoch directories.
pub fn ltx_root(scope: &str) -> String {
    format!("cells/{scope}/ltx/")
}

/// `cells/{scope}/ltx/e{epoch}/{level:04x}/`.
pub fn level_prefix(scope: &str, epoch: u64, level: u32) -> String {
    format!("cells/{scope}/ltx/e{epoch}/{level:04x}/")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCOPE: &str = "Counter:1182fbf2bee1753c4ffbfddea36c7eff522bfe47006c0f9f7e6035db0fc0b40d";

    #[test]
    fn parses_an_encoded_event_key() {
        let raw = "cells/Counter%3A1182fbf2bee1753c4ffbfddea36c7eff522bfe47006c0f9f7e6035db0fc0b40d/ltx/e3/0001/0000000000000001-00000000000000ff.ltx";
        let Some(CelldKey::Ltx(k)) = parse("", &decode_event_key(raw)) else {
            panic!("not an ltx key");
        };
        assert_eq!(
            k,
            LtxKey {
                scope: SCOPE.into(),
                epoch: 3,
                level: 1,
                min_txid: 1,
                max_txid: 255
            }
        );
        assert_eq!(k.to_key(""), decode_event_key(raw));
    }

    #[test]
    fn parses_a_bundle_key_under_a_prefix() {
        let key = "fleet1/log/node_a/b4a9/bundle/e1-00000000.ltxb";
        let Some(CelldKey::Bundle(b)) = parse("fleet1/", key) else {
            panic!("not a bundle key");
        };
        assert_eq!(b.node, "node_a");
        assert_eq!(b.to_key("fleet1/"), key);
    }

    #[test]
    fn ignores_other_objects() {
        for key in [
            "cells/C:1/own.json",
            "cells/C:1/ltx/retired.json",
            "cells/C:1/facets/ab/ltx/e1/0000/0000000000000001-0000000000000001.ltx",
            "cells/C:1/ltx/e1/0000/0000000000000002-0000000000000001.ltx",
            "deploy/current.json",
            "log/n/g/bundle/e1-0.json",
        ] {
            assert_eq!(parse("", key), None, "{key}");
        }
    }
}
