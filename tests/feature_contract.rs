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
fn public_research_context_capabilities_are_implemented_for_v020() {
    for id in [
        "clob.prices.batchRead",
        "clob.midpoints.batchRead",
        "clob.spreads.batchRead",
        "clob.lastTrades.batchRead",
        "tags.list",
        "series.list",
        "sports.list",
        "sports.marketTypes.list",
        "sports.teams.list",
        "intel.wallet.dossier",
        "intel.marketFlow.read",
    ] {
        assert_eq!(
            CapabilityCatalog::by_id(id).unwrap().status,
            CapabilityStatus::Implemented,
            "{id}"
        );
    }
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.2.0");
}

#[test]
fn reported_capabilities_stay_sorted() {
    let ids = ids();
    assert!(ids.windows(2).all(|pair| pair[0] <= pair[1]));
}
