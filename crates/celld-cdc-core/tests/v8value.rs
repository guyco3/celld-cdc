//! `decode_kv_value` against payloads from Node's V8 `v8.Serializer`.

use celld_cdc_core::v8value::decode_kv_value;
use serde_json::Value;

#[test]
fn node_fixtures() {
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/v8/node_values.json")).unwrap();
    assert!(fixtures.len() > 40);
    let mut failures = Vec::new();
    for f in &fixtures {
        let bytes = hex::decode(f["hex"].as_str().unwrap()).unwrap();
        let got = decode_kv_value(&bytes);
        if got != f["expected"] {
            failures.push(format!("{}: got {got}, want {}", f["name"], f["expected"]));
        }
        let mut stub = vec![0x01];
        stub.extend(&bytes);
        assert_eq!(
            decode_kv_value(&stub)["$stubs"],
            f["expected"],
            "{}",
            f["name"]
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn truncated_fixtures_fall_back_to_raw() {
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/v8/node_values.json")).unwrap();
    for f in &fixtures {
        let bytes = hex::decode(f["hex"].as_str().unwrap()).unwrap();
        for n in 0..bytes.len() {
            let v = decode_kv_value(&bytes[..n]);
            // An ArrayBuffer cut off before its view is itself a whole value.
            if bytes[n] == b'V' {
                continue;
            }
            assert!(
                v.get("$v8").is_some(),
                "{} truncated to {n}: {v}",
                f["name"]
            );
        }
    }
}
