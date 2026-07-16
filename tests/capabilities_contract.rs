use polyrover::capabilities::CapabilityCatalog;
use serde_json::{Map, Value};
use std::{collections::BTreeSet, path::Path};

const MANIFEST: &str = include_str!("../capabilities.json");
const CATALOG_FIELDS: &[&str] = &[
    "id",
    "tier",
    "service",
    "operation",
    "transport",
    "auth",
    "signing",
    "mutates",
    "cliCommand",
    "extension",
    "summary",
    "status",
];

fn project(value: &Value) -> Value {
    let source = value.as_object().expect("capability object");
    Value::Object(
        CATALOG_FIELDS
            .iter()
            .map(|key| ((*key).to_owned(), source[*key].clone()))
            .collect::<Map<String, Value>>(),
    )
}

#[test]
fn compiled_catalog_matches_manifest() {
    let manifest: Value = serde_json::from_str(MANIFEST).expect("valid manifest JSON");
    let expected = manifest["capabilities"]
        .as_array()
        .expect("capabilities array")
        .iter()
        .map(project)
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), 136);
    let serialized = serde_json::to_value(CapabilityCatalog::all()).expect("catalog serializes");
    let actual = serialized
        .as_array()
        .expect("catalog array")
        .iter()
        .map(project)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn implemented_evidence_paths_exist() {
    let manifest: Value = serde_json::from_str(MANIFEST).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for cap in manifest["capabilities"].as_array().unwrap() {
        if cap["status"] == "implemented" {
            for test in cap["tests"].as_array().unwrap() {
                assert!(
                    root.join(test.as_str().unwrap()).exists(),
                    "{}: missing {}",
                    cap["id"],
                    test
                );
            }
        }
    }
}

#[test]
fn ids_are_unique_and_lookup_round_trips() {
    let all = CapabilityCatalog::all();
    let ids = all.iter().map(|cap| cap.id).collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), all.len());
    for cap in all {
        assert_eq!(CapabilityCatalog::by_id(cap.id), Some(cap));
    }
}
