#![cfg(feature = "public")]

use polyrover::{
    clob::{BatchPriceHistoryParams, PriceHistoryParams},
    Client, ClientConfig,
};

fn token_id() -> String {
    std::env::var("POLYROVER_CANARY_TOKEN_ID")
        .expect("set POLYROVER_CANARY_TOKEN_ID to a public CLOB asset ID")
}

#[tokio::test]
#[ignore = "manual public API canary; never run in ordinary CI"]
async fn live_single_price_history_matches_the_typed_contract() {
    let history = Client::new(ClientConfig::default())
        .unwrap()
        .price_history(&PriceHistoryParams {
            token_id: token_id(),
            interval: Some("1d".into()),
            fidelity: Some(60),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(history
        .history
        .iter()
        .all(|point| !point.price.trim().is_empty()));
}

#[tokio::test]
#[ignore = "manual public API canary; never run in ordinary CI"]
async fn live_batch_price_history_matches_the_typed_contract() {
    let token_id = token_id();
    let history = Client::new(ClientConfig::default())
        .unwrap()
        .batch_price_history(&BatchPriceHistoryParams {
            markets: vec![token_id.clone()],
            interval: Some("1d".into()),
            fidelity: Some(60),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(history.history.contains_key(&token_id));
}
