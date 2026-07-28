#![cfg(feature = "public")]

use std::{fs, path::Path};

use polyrover::types::{ClobBatchPriceHistory, ClobPriceHistory};
use serde_json::Value;

#[test]
fn clob_history_contract_examples_deserialize_and_preserve_decimal_text() {
    let single: ClobPriceHistory =
        serde_json::from_str(include_str!("fixtures/clob/price-history.json")).unwrap();
    let batch: ClobBatchPriceHistory =
        serde_json::from_str(include_str!("fixtures/clob/batch-price-history.json")).unwrap();

    assert_eq!(single.history[0].price, "0.42");
    assert_eq!(single.history[1].price, "0.425");
    assert_eq!(batch.history["token-1"][0].timestamp, 1_700_000_000);
}

#[test]
fn every_contract_fixture_has_explicit_provenance() {
    let manifest: Value = serde_json::from_str(include_str!("fixtures/provenance.json")).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let fixtures = manifest["fixtures"].as_array().unwrap();

    assert_eq!(manifest["schemaVersion"], 1);
    assert_eq!(fixtures.len(), 2);
    for fixture in fixtures {
        let relative = fixture["path"].as_str().unwrap();
        assert!(root.join(relative).is_file(), "missing fixture {relative}");
        assert_eq!(fixture["kind"], "contract-example");
        assert_eq!(fixture["containsLiveData"], false);
        assert!(fixture["source"]
            .as_str()
            .is_some_and(|source| source.starts_with("https://")));
        assert!(!fs::read_to_string(root.join(relative)).unwrap().is_empty());
    }
}
