use polyrover::capabilities::{CapabilityCatalog, CapabilityStatus};

fn ids() -> Vec<&'static str> {
    CapabilityCatalog::all()
        .iter()
        .map(|capability| capability.id)
        .collect()
}

#[test]
fn catalog_reports_source_capabilities_independently_of_compiled_features() {
    let ids = ids();
    assert!(ids.contains(&"data.positions.read"));
    assert!(ids.contains(&"stream.user.subscribe"));
    assert!(ids.contains(&"clob.orders.limit.submit"));
    assert!(ids.contains(&"bridge.assets.read"));
}

#[test]
fn price_history_capability_is_implemented() {
    let capability = CapabilityCatalog::by_id("clob.priceHistory.read").unwrap();
    assert_eq!(capability.status, CapabilityStatus::Implemented);
}

#[test]
fn reported_capabilities_stay_sorted() {
    let ids = ids();
    assert!(ids.windows(2).all(|pair| pair[0] <= pair[1]));
}
