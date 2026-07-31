#![cfg(feature = "authenticated")]

use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
};

use polyrover::{
    auth::{sign_hmac, ApiKey, L2Credentials},
    authenticated_clob::{Client, OrderParams, RewardParams, RewardsMarketParams, TradeParams},
};

fn credentials() -> L2Credentials {
    L2Credentials {
        address: "0x1234567890123456789012345678901234567890".into(),
        api_key: ApiKey {
            key: "key".into(),
            secret: "secret".into(),
            passphrase: "pass".into(),
        },
    }
}

fn serve_json(
    bodies: Vec<&'static str>,
) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (requests, received) = mpsc::channel();
    let server = thread::spawn(move || {
        for body in bodies {
            let (mut stream, _) = listener.accept().unwrap();
            let mut raw = [0; 8192];
            let length = stream.read(&mut raw).unwrap();
            requests
                .send(String::from_utf8_lossy(&raw[..length]).into_owned())
                .unwrap();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        }
    });
    (format!("http://{address}"), received, server)
}

fn header<'a>(request: &'a str, name: &str) -> &'a str {
    request
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
        .unwrap_or_else(|| panic!("missing {name}"))
}

#[tokio::test]
async fn authenticated_trade_page_signs_canonical_path_and_preserves_cursor() {
    let (base_url, received, server) = serve_json(vec![
        r#"{"limit":100,"next_cursor":"next==","count":0,"data":[]}"#,
    ]);
    let client = Client::new(base_url).unwrap();
    let credentials = credentials();
    let page = client
        .trades_page(
            &credentials,
            &TradeParams {
                market: "market-1".into(),
                after: Some(1),
                next_cursor: "cursor==".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(page.next_cursor, "next==");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /data/trades?"));
    assert!(request.contains("market=market-1"));
    assert!(request.contains("after=1"));
    assert!(request.contains("next_cursor=cursor%3D%3D"));
    assert_eq!(header(&request, "poly_address"), credentials.address);
    let timestamp = header(&request, "poly_timestamp").parse().unwrap();
    assert_eq!(
        header(&request, "poly_signature"),
        sign_hmac("secret", timestamp, "GET", "/data/trades", None)
    );
    server.join().unwrap();
}

#[tokio::test]
async fn authenticated_reward_history_reads_are_atomic_and_canonically_signed() {
    let (base_url, received, server) = serve_json(vec![
        r#"{"limit":100,"next_cursor":"next==","count":1,"data":[{"date":"2026-07-30","earnings":12.34}]}"#,
        r#"[{"date":"2026-07-30","earnings":12.34}]"#,
        r#"{"limit":100,"next_cursor":"done","count":1,"data":[{"condition_id":"condition-1","market_id":"market-1","question":"Question?","tokens":[]}]}"#,
    ]);
    let client = Client::new(base_url).unwrap();
    let credentials = credentials();
    let params = RewardParams {
        date: "2026-07-30".into(),
        signature_type: Some(2),
        maker_address: "0xmaker".into(),
        sponsored: Some(false),
        next_cursor: "cursor==".into(),
    };

    let page = client.rewards_page(&credentials, &params).await.unwrap();
    assert_eq!(page.data[0].earnings, "12.34");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /rewards/user?date=2026-07-30&signature_type=2&maker_address=0xmaker&sponsored=false&next_cursor=cursor%3D%3D "));
    let timestamp = header(&request, "poly_timestamp").parse().unwrap();
    assert_eq!(
        header(&request, "poly_signature"),
        sign_hmac("secret", timestamp, "GET", "/rewards/user", None)
    );

    let totals = client.rewards_total(&credentials, &params).await.unwrap();
    assert_eq!(totals[0].earnings, "12.34");
    assert!(received.recv().unwrap().starts_with(
        "GET /rewards/user/total?date=2026-07-30&signature_type=2&maker_address=0xmaker&sponsored=false "
    ));

    let markets = client
        .rewards_markets_page(
            &credentials,
            &RewardsMarketParams {
                date: "2026-07-30".into(),
                next_cursor: "cursor==".into(),
                page_size: Some(100),
                only_open_positions: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(markets.data[0].condition_id, "condition-1");
    assert!(received.recv().unwrap().starts_with(
        "GET /rewards/user/markets?date=2026-07-30&next_cursor=cursor%3D%3D&page_size=100&only_open_positions=true "
    ));
    server.join().unwrap();
}

#[tokio::test]
async fn authenticated_orders_page_and_lookup_are_atomic() {
    let (base_url, received, server) = serve_json(vec![
        r#"{"limit":100,"next_cursor":"done","count":0,"data":[]}"#,
        r#"{"id":"order/with space","status":"ORDER_STATUS_LIVE","price":"0.5"}"#,
    ]);
    let client = Client::new(base_url).unwrap();
    let credentials = credentials();

    let page = client
        .orders_page(
            &credentials,
            &OrderParams {
                asset_id: "asset-1".into(),
                next_cursor: "cursor==".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.next_cursor, "done");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /data/orders?asset_id=asset-1&next_cursor=cursor%3D%3D "));
    let timestamp = header(&request, "poly_timestamp").parse().unwrap();
    assert_eq!(
        header(&request, "poly_signature"),
        sign_hmac("secret", timestamp, "GET", "/data/orders", None)
    );

    let order = client
        .order(&credentials, "order/with space")
        .await
        .unwrap();
    assert_eq!(order.id, "order/with space");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /data/order/order%2Fwith%20space "));
    server.join().unwrap();
}
