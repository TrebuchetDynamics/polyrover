#![cfg(feature = "public")]

use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
};

use chrono::{TimeZone, Utc};

use polyrover::{
    clob::{BatchMarketRequest, BatchPriceHistoryParams, PriceHistoryParams},
    data::{ActivityParams, ClosedPositionParams, LeaderboardParams, TradeParams},
    gamma::{MarketKeysetParams, MarketParams, SearchParams, TaxonomyParams, TeamParams},
    simulation::Request,
    stream::{parse_market_event, MarketEvent},
    Client, ClientConfig,
};

fn serve_json(body: &'static str) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (requests, received) = mpsc::channel();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut raw = [0; 4096];
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
    });
    (format!("http://{address}"), received, handle)
}

fn serve_sequence(
    responses: Vec<&'static str>,
) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (requests, received) = mpsc::channel();
    let handle = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let mut raw = [0; 4096];
            let length = stream.read(&mut raw).unwrap();
            requests
                .send(String::from_utf8_lossy(&raw[..length]).into_owned())
                .unwrap();
            stream.write_all(response.as_bytes()).unwrap();
        }
    });
    (format!("http://{address}"), received, handle)
}

#[tokio::test]
async fn client_searches_markets_through_one_public_interface() {
    let (gamma_base_url, received, server) =
        serve_json(r#"{"events":[{"id":"event-1","title":"Bitcoin"}]}"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let response = client
        .search(&SearchParams {
            q: "bitcoin".into(),
            limit_per_type: Some(1),
            ..SearchParams::default()
        })
        .await
        .unwrap();

    assert_eq!(response.events[0].id, "event-1");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /public-search?"));
    assert!(request.contains("q=bitcoin"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_clob_books_through_one_public_interface() {
    let (clob_base_url, received, server) =
        serve_json(r#"{"asset_id":"token-1","bids":[],"asks":[]}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let book = client.order_book("token-1").await.unwrap();

    assert_eq!(book.asset_id, "token-1");
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /book?token_id=token-1 "));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_clob_books_in_one_batch() {
    let (clob_base_url, received, server) = serve_json(
        r#"[{"asset_id":"token-1","bids":[],"asks":[]},{"asset_id":"token-2","bids":[],"asks":[]}]"#,
    );
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let books = client
        .order_books(&["token-1".into(), "token-2".into()])
        .await
        .unwrap();

    assert_eq!(books.len(), 2);
    let request = received.recv().unwrap();
    assert!(request.starts_with("POST /books "));
    assert!(request.contains(r#"{"token_id":"token-1"}"#));
    assert!(request.contains(r#"{"token_id":"token-2"}"#));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_batch_prices_as_decimal_text() {
    let (clob_base_url, received, server) =
        serve_json(r#"{"token-1":{"BUY":0.45},"token-2":{"SELL":"0.52"}}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .batch_prices(&[
            BatchMarketRequest::new("token-1", "BUY"),
            BatchMarketRequest::new("token-2", "SELL"),
        ])
        .await
        .unwrap();

    assert_eq!(rows["token-1"]["BUY"], "0.45");
    assert_eq!(rows["token-2"]["SELL"], "0.52");
    let request = received.recv().unwrap();
    assert!(request.starts_with("POST /prices "));
    assert!(request.contains(r#""token_id":"token-1""#));
    assert!(request.contains(r#""side":"BUY""#));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_batch_midpoints_as_decimal_text() {
    let (clob_base_url, received, server) = serve_json(r#"{"token-1":"0.45","token-2":0.52}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .batch_midpoints(&[
            BatchMarketRequest::new("token-1", ""),
            BatchMarketRequest::new("token-2", ""),
        ])
        .await
        .unwrap();

    assert_eq!(rows["token-1"], "0.45");
    assert_eq!(rows["token-2"], "0.52");
    assert!(received.recv().unwrap().starts_with("POST /midpoints "));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_batch_spreads_as_decimal_text() {
    let (clob_base_url, received, server) = serve_json(r#"{"token-1":"0.02","token-2":0.015}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .batch_spreads(&[
            BatchMarketRequest::new("token-1", ""),
            BatchMarketRequest::new("token-2", ""),
        ])
        .await
        .unwrap();

    assert_eq!(rows["token-1"], "0.02");
    assert_eq!(rows["token-2"], "0.015");
    assert!(received.recv().unwrap().starts_with("POST /spreads "));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_batch_last_trades_as_decimal_text() {
    let (clob_base_url, received, server) = serve_json(
        r#"[{"token_id":"token-1","price":"0.45","side":"BUY"},{"token_id":"token-2","price":0.52,"side":"SELL"}]"#,
    );
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .batch_last_trades(&[
            BatchMarketRequest::new("token-1", ""),
            BatchMarketRequest::new("token-2", ""),
        ])
        .await
        .unwrap();

    assert_eq!(rows[0].price, "0.45");
    assert_eq!(rows[1].price, "0.52");
    assert!(received
        .recv()
        .unwrap()
        .starts_with("POST /last-trades-prices "));
    server.join().unwrap();
}

#[tokio::test]
async fn batch_context_rejects_empty_requests_and_invalid_price_sides() {
    let client = polyrover::clob::Client::new("http://127.0.0.1:1").unwrap();
    assert!(client.batch_midpoints(&[]).await.is_err());
    let error = client
        .batch_prices(&[BatchMarketRequest::new("token-1", "hold")])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("BUY or SELL"));
}

#[tokio::test]
async fn public_batch_read_post_retries_429() {
    let body = r#"[{"asset_id":"token-1","bids":[],"asks":[]}]"#;
    let success = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let success: &'static str = Box::leak(success.into_boxed_str());
    let (clob_base_url, received, server) = serve_sequence(vec![
        "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        success,
    ]);
    let client = Client::new(ClientConfig {
        clob_base_url,
        http_retry: polyrover::transport::RetryPolicy {
            max_retries: 1,
            base_delay_ms: 0,
            max_delay_ms: 0,
        },
        ..ClientConfig::default()
    })
    .unwrap();

    let books = client.order_books(&["token-1".into()]).await.unwrap();

    assert_eq!(books.len(), 1);
    assert_eq!(received.iter().take(2).count(), 2);
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_positions_through_one_public_interface() {
    let (data_base_url, received, server) = serve_json(r#"[{"asset":"token-1"}]"#);
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let positions = client.current_positions("0xuser", 5).await.unwrap();

    assert_eq!(positions[0].token_id, "token-1");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /positions?"));
    assert!(request.contains("user=0xuser"));
    assert!(request.contains("limit=5"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_lists_markets_through_one_public_interface() {
    let (gamma_base_url, received, server) = serve_json("[]");
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let markets = client
        .markets(&MarketParams {
            limit: Some(5),
            ..MarketParams::default()
        })
        .await
        .unwrap();

    assert!(markets.is_empty());
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /markets?"));
    assert!(request.contains("limit=5"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_gamma_taxonomy() {
    let page = TaxonomyParams {
        limit: Some(10),
        offset: Some(0),
        ..Default::default()
    };

    let (gamma_base_url, received, server) = serve_json(r#"[{"id":"7"}]"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(client.tags(&page).await.unwrap()[0].id, "7");
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /tags?limit=10&offset=0 "));
    server.join().unwrap();

    let (gamma_base_url, received, server) = serve_json(r#"{"id":"7"}"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(client.tag_by_id(7).await.unwrap().id, "7");
    assert!(received.recv().unwrap().starts_with("GET /tags/7 "));
    server.join().unwrap();

    let (gamma_base_url, received, server) = serve_json(r#"{"slug":"crypto"}"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(client.tag_by_slug("crypto").await.unwrap().slug, "crypto");
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /tags/slug/crypto "));
    server.join().unwrap();

    let (gamma_base_url, received, server) = serve_json(r#"[{"id":"12"}]"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(client.series(&page).await.unwrap()[0].id, "12");
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /series?limit=10&offset=0 "));
    server.join().unwrap();

    let (gamma_base_url, received, server) = serve_json(r#"{"id":"12"}"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(client.series_by_id(12).await.unwrap().id, "12");
    assert!(received.recv().unwrap().starts_with("GET /series/12 "));
    server.join().unwrap();

    let (gamma_base_url, received, server) = serve_json(r#"[{"sport":"nfl"}]"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(client.sports().await.unwrap()[0].sport, "nfl");
    assert!(received.recv().unwrap().starts_with("GET /sports "));
    server.join().unwrap();

    let (gamma_base_url, received, server) = serve_json(r#"{"marketTypes":["moneyline"]}"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        client.sports_market_types().await.unwrap().market_types,
        ["moneyline"]
    );
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /sports/market-types "));
    server.join().unwrap();

    let (gamma_base_url, received, server) = serve_json(r#"[{"id":123}]"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..Default::default()
    })
    .unwrap();
    let teams = client
        .teams(&TeamParams {
            page,
            leagues: vec!["NFL".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(teams[0].id, 123);
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /teams?limit=10&offset=0&league=NFL "));
    server.join().unwrap();
}

#[tokio::test]
async fn client_pages_gamma_markets_with_keyset_cursor() {
    let (gamma_base_url, received, server) =
        serve_json(r#"{"markets":[{"id":"market-1"}],"next_cursor":"opaque next"}"#);
    let client = Client::new(ClientConfig {
        gamma_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let page = client
        .market_page(&MarketKeysetParams {
            limit: Some(100),
            after_cursor: "opaque previous".into(),
            closed: Some(true),
            ..MarketKeysetParams::default()
        })
        .await
        .unwrap();

    assert_eq!(page.markets[0].id, "market-1");
    assert_eq!(page.next_cursor, "opaque next");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /markets/keyset?"));
    assert!(request.contains("limit=100"));
    assert!(request.contains("after_cursor=opaque%20previous"));
    assert!(request.contains("closed=true"));
    server.join().unwrap();
}

#[tokio::test]
async fn public_get_retries_429_using_fractional_retry_after() {
    let (clob_base_url, received, server) = serve_sequence(vec![
        "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0.001\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\nConnection: close\r\n\r\n{\"price\":\"0.42\"}",
    ]);
    let client = Client::new(ClientConfig {
        clob_base_url,
        http_retry: polyrover::transport::RetryPolicy {
            max_retries: 1,
            base_delay_ms: 0,
            max_delay_ms: 1,
        },
        ..ClientConfig::default()
    })
    .unwrap();

    assert_eq!(client.price("token-1", "buy").await.unwrap(), "0.42");
    assert_eq!(received.iter().take(2).count(), 2);
    server.join().unwrap();
}

#[tokio::test]
async fn public_get_retries_425() {
    let (clob_base_url, received, server) = serve_sequence(vec![
        "HTTP/1.1 425 Too Early\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\nConnection: close\r\n\r\n{\"price\":\"0.43\"}",
    ]);
    let client = Client::new(ClientConfig {
        clob_base_url,
        http_retry: polyrover::transport::RetryPolicy {
            max_retries: 1,
            base_delay_ms: 0,
            max_delay_ms: 0,
        },
        ..ClientConfig::default()
    })
    .unwrap();

    assert_eq!(client.price("token-1", "buy").await.unwrap(), "0.43");
    assert_eq!(received.iter().take(2).count(), 2);
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_clob_prices_through_one_public_interface() {
    let (clob_base_url, received, server) = serve_json(r#"{"price":"0.42"}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let price = client.price("token-1", "buy").await.unwrap();

    assert_eq!(price, "0.42");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /price?"));
    assert!(request.contains("token_id=token-1"));
    assert!(request.contains("side=buy"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_typed_clob_price_history() {
    let (clob_base_url, received, server) =
        serve_json(r#"{"history":[{"t":1700000000,"p":0.42}]}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let history = client
        .price_history(&PriceHistoryParams {
            token_id: "token-1".into(),
            start_ts: Some(1_700_000_000),
            end_ts: Some(1_700_003_600),
            interval: Some("1h".into()),
            fidelity: Some(5),
        })
        .await
        .unwrap();

    assert_eq!(history.history[0].timestamp, 1_700_000_000);
    assert_eq!(history.history[0].price, "0.42");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /prices-history?"));
    assert!(request.contains("market=token-1"));
    assert!(request.contains("startTs=1700000000"));
    assert!(request.contains("endTs=1700003600"));
    assert!(request.contains("interval=1h"));
    assert!(request.contains("fidelity=5"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_clob_price_history_in_one_batch() {
    let (clob_base_url, received, server) =
        serve_json(r#"{"history":{"token-1":[{"t":1700000000,"p":"0.42"}],"token-2":[]}}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let history = client
        .batch_price_history(&BatchPriceHistoryParams {
            markets: vec!["token-1".into(), "token-2".into()],
            start_ts: Some(1_700_000_000),
            interval: Some("1h".into()),
            fidelity: Some(5),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(history.history["token-1"][0].price, "0.42");
    let request = received.recv().unwrap();
    assert!(request.starts_with("POST /batch-prices-history "));
    assert!(request.contains(r#""markets":["token-1","token-2"]"#));
    assert!(request.contains(r#""start_ts":1700000000"#));
    assert!(!request.contains("end_ts"));
    server.join().unwrap();
}

#[tokio::test]
async fn batch_price_history_rejects_more_than_twenty_markets() {
    let client = polyrover::clob::Client::new("http://127.0.0.1:1").unwrap();
    let error = client
        .batch_price_history(&BatchPriceHistoryParams {
            markets: (0..21).map(|index| format!("token-{index}")).collect(),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert!(error.to_string().contains("1..=20 markets"));
}

#[tokio::test]
async fn batch_price_history_rejects_empty_or_blank_markets() {
    let client = polyrover::clob::Client::new("http://127.0.0.1:1").unwrap();
    for markets in [Vec::new(), vec![" ".into()]] {
        let error = client
            .batch_price_history(&BatchPriceHistoryParams {
                markets,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("market"));
        assert!(!error.is_retriable());
    }
}

#[tokio::test]
async fn client_reads_fee_rate_through_one_public_interface() {
    let (clob_base_url, received, server) = serve_json(r#"{"base_fee":30}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let fee = client.fee_rate("token-1").await.unwrap();

    assert_eq!(fee.base_fee_bps, 30);
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /fee-rate?token_id=token-1 "));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_crypto_reference_price_through_one_public_interface() {
    let (crypto_price_base_url, received, server) = serve_json(
        r#"{"openPrice":64000.5,"closePrice":64010.25,"timestamp":1778745300000,"completed":false,"incomplete":false,"cached":true}"#,
    );
    let client = Client::new(ClientConfig {
        crypto_price_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let price = client
        .crypto_price(
            "btc",
            Utc.with_ymd_and_hms(2026, 5, 14, 7, 55, 0).unwrap(),
            "fiveminute",
            Utc.with_ymd_and_hms(2026, 5, 14, 8, 0, 0).unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(price.open_price, Some(64000.5));
    assert_eq!(price.close_price, Some(64010.25));
    assert!(price.cached);
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /api/crypto/crypto-price?"));
    assert!(request.contains("symbol=BTC"));
    assert!(request.contains("eventStartTime=2026-05-14T07%3A55%3A00Z"));
    assert!(request.contains("variant=fiveminute"));
    assert!(request.contains("endDate=2026-05-14T08%3A00%3A00Z"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_accepts_missing_future_crypto_reference_price() {
    let (crypto_price_base_url, received, server) = serve_json(
        r#"{"openPrice":null,"closePrice":null,"timestamp":0,"completed":false,"incomplete":true,"cached":false}"#,
    );
    let client = Client::new(ClientConfig {
        crypto_price_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let price = client
        .crypto_price(
            "BTC",
            Utc.with_ymd_and_hms(2026, 5, 14, 8, 0, 0).unwrap(),
            "fiveminute",
            Utc.with_ymd_and_hms(2026, 5, 14, 8, 5, 0).unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(price.open_price, None);
    assert!(price.incomplete);
    received.recv().unwrap();
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_trades_through_one_public_interface() {
    let (data_base_url, received, server) = serve_json("[]");
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let trades = client.trades("0xuser", 7).await.unwrap();

    assert!(trades.is_empty());
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /trades?"));
    assert!(request.contains("user=0xuser"));
    assert!(request.contains("limit=7"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_market_trades_through_one_public_interface() {
    let (data_base_url, received, server) = serve_json("[]");
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let trades = client.market_trades("0xmarket", 6).await.unwrap();

    assert!(trades.is_empty());
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /trades?"));
    assert!(request.contains("market=0xmarket"));
    assert!(request.contains("limit=6"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_top_holders_through_one_public_interface() {
    let (data_base_url, received, server) =
        serve_json(r#"[{"holders":[{"proxyWallet":"0xholder","amount":12}]}]"#);
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let holders = client.top_holders("0xmarket", 7).await.unwrap();

    assert_eq!(holders[0].address, "0xholder");
    assert_eq!(holders[0].shares, 12.0);
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /holders?"));
    assert!(request.contains("market=0xmarket"));
    assert!(request.contains("limit=7"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_total_value_through_one_public_interface() {
    let (data_base_url, received, server) = serve_json(r#"{"value":42.5}"#);
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let value = client.total_value("0xuser").await.unwrap();

    assert_eq!(value.user, "0xuser");
    assert_eq!(value.value, 42.5);
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /value?user=0xuser "));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_open_interest_through_one_public_interface() {
    let (data_base_url, received, server) = serve_json(r#"[{"market":"0xmarket","value":1250}]"#);
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let interest = client.open_interest("0xmarket").await.unwrap();

    assert_eq!(interest.market, "0xmarket");
    assert_eq!(interest.open_value, 1250.0);
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /oi?market=0xmarket "));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_leaderboard_through_one_public_interface() {
    let (data_base_url, received, server) = serve_json("[]");
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client.trader_leaderboard(9).await.unwrap();

    assert!(rows.is_empty());
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /v1/leaderboard?"));
    assert!(request.contains("limit=9"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_filtered_leaderboard_pages() {
    let (data_base_url, received, server) =
        serve_json(r#"[{"rank":"1","proxyWallet":"0xabc","userName":"alice","vol":100,"pnl":25}]"#);
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .trader_leaderboard_with(&LeaderboardParams {
            category: "POLITICS".into(),
            time_period: "MONTH".into(),
            order_by: "PNL".into(),
            limit: Some(50),
            offset: Some(100),
            ..LeaderboardParams::default()
        })
        .await
        .unwrap();

    assert_eq!(rows[0].proxy_wallet, "0xabc");
    assert_eq!(rows[0].user_name, "alice");
    assert_eq!(rows[0].user, "0xabc");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /v1/leaderboard?"));
    assert!(request.contains("category=POLITICS"));
    assert!(request.contains("timePeriod=MONTH"));
    assert!(request.contains("orderBy=PNL"));
    assert!(request.contains("limit=50"));
    assert!(request.contains("offset=100"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_filtered_closed_position_pages() {
    let (data_base_url, received, server) = serve_json(
        r#"[{"proxyWallet":"0xabc","asset":"token-1","conditionId":"0xmarket","realizedPnl":25,"timestamp":123}]"#,
    );
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .closed_positions_with(&ClosedPositionParams {
            user: "0xabc".into(),
            markets: vec!["0xmarket".into(), "0xmarket2".into()],
            limit: Some(50),
            offset: Some(150),
            sort_by: "REALIZEDPNL".into(),
            sort_direction: "DESC".into(),
            ..ClosedPositionParams::default()
        })
        .await
        .unwrap();

    assert_eq!(rows[0].position.proxy_wallet, "0xabc");
    assert_eq!(rows[0].position.realized_pnl, 25.0);
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /closed-positions?"));
    assert!(request.contains("user=0xabc"));
    assert!(request.contains("market=0xmarket%2C0xmarket2"));
    assert!(request.contains("limit=50"));
    assert!(request.contains("offset=150"));
    assert!(request.contains("sortBy=REALIZEDPNL"));
    assert!(request.contains("sortDirection=DESC"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_filtered_trade_pages() {
    let (data_base_url, received, server) = serve_json(
        r#"[{"proxyWallet":"0xabc","asset":"token-1","conditionId":"0xmarket","transactionHash":"0xtx","timestamp":123}]"#,
    );
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .trades_with(&TradeParams {
            user: "0xabc".into(),
            side: "BUY".into(),
            start: Some(1),
            end: Some(123),
            limit: Some(100),
            offset: Some(200),
            ..TradeParams::default()
        })
        .await
        .unwrap();

    assert_eq!(rows[0].proxy_wallet, "0xabc");
    assert_eq!(rows[0].asset_id, "token-1");
    assert_eq!(rows[0].market, "0xmarket");
    assert_eq!(rows[0].transaction_hash, "0xtx");
    assert_eq!(rows[0].created_at, "123");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /trades?"));
    assert!(request.contains("user=0xabc"));
    assert!(request.contains("side=BUY"));
    assert!(request.contains("start=1"));
    assert!(request.contains("end=123"));
    assert!(request.contains("limit=100"));
    assert!(request.contains("offset=200"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reads_filtered_activity_pages() {
    let (data_base_url, received, server) = serve_json(
        r#"[{"proxyWallet":"0xabc","type":"TRADE","conditionId":"0xmarket","usdcSize":50,"transactionHash":"0xtx","timestamp":123}]"#,
    );
    let client = Client::new(ClientConfig {
        data_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let rows = client
        .activity_with(&ActivityParams {
            user: "0xabc".into(),
            activity_types: vec!["TRADE".into(), "REDEEM".into()],
            sort_by: "TIMESTAMP".into(),
            sort_direction: "DESC".into(),
            limit: Some(100),
            offset: Some(300),
            ..ActivityParams::default()
        })
        .await
        .unwrap();

    assert_eq!(rows[0].proxy_wallet, "0xabc");
    assert_eq!(rows[0].condition_id, "0xmarket");
    assert_eq!(rows[0].transaction_hash, "0xtx");
    assert_eq!(rows[0].usdc_size, "50");
    let request = received.recv().unwrap();
    assert!(request.starts_with("GET /activity?"));
    assert!(request.contains("user=0xabc"));
    assert!(request.contains("type=TRADE%2CREDEEM"));
    assert!(request.contains("sortBy=TIMESTAMP"));
    assert!(request.contains("sortDirection=DESC"));
    assert!(request.contains("limit=100"));
    assert!(request.contains("offset=300"));
    server.join().unwrap();
}

#[tokio::test]
async fn client_reports_combined_health_through_one_public_interface() {
    let (gamma_base_url, gamma_request, gamma_server) = serve_json("{}");
    let (clob_base_url, clob_request, clob_server) = serve_json("{}");
    let client = Client::new(ClientConfig {
        gamma_base_url,
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let health = client.health().await;

    assert_eq!(health.gamma, "ok");
    assert_eq!(health.clob, "ok");
    assert!(gamma_request.recv().unwrap().starts_with("GET / "));
    assert!(clob_request.recv().unwrap().starts_with("GET / "));
    gamma_server.join().unwrap();
    clob_server.join().unwrap();
}

#[tokio::test]
async fn client_simulates_fills_through_one_public_interface() {
    let (clob_base_url, received, server) =
        serve_json(r#"{"asset_id":"token-1","asks":[{"price":"0.5","size":"10"}]}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let fill = client
        .simulate(Request {
            token_id: "token-1".into(),
            side: "buy".into(),
            amount: "1".into(),
            limit_price: String::new(),
        })
        .await
        .unwrap();

    assert!(fill.complete);
    assert_eq!(fill.filled_size, "2");
    assert!(received
        .recv()
        .unwrap()
        .starts_with("GET /book?token_id=token-1 "));
    server.join().unwrap();
}

#[tokio::test]
async fn client_simulates_fills_with_documented_fee_category() {
    let (clob_base_url, _received, server) =
        serve_json(r#"{"asset_id":"token-1","asks":[{"price":"0.5","size":"100"}]}"#);
    let client = Client::new(ClientConfig {
        clob_base_url,
        ..ClientConfig::default()
    })
    .unwrap();

    let fill = client
        .simulate_with_fee(
            Request {
                token_id: "token-1".into(),
                side: "buy".into(),
                amount: "50".into(),
                limit_price: String::new(),
            },
            "crypto",
        )
        .await
        .unwrap();

    assert_eq!(fill.estimated_taker_fee, "1.75");
    server.join().unwrap();
}

#[test]
fn parses_existing_typed_market_events() {
    assert!(matches!(
        parse_market_event(r#"{"event_type":"book"}"#),
        Ok(MarketEvent::Book(_))
    ));
    assert!(matches!(
        parse_market_event(r#"{"event_type":"price_change"}"#),
        Ok(MarketEvent::PriceChange(_))
    ));
    assert!(matches!(
        parse_market_event(r#"{"event_type":"last_trade_price"}"#),
        Ok(MarketEvent::LastTrade(_))
    ));
    assert!(matches!(
        parse_market_event(r#"{"event_type":"tick_size_change"}"#),
        Ok(MarketEvent::TickSizeChange(_))
    ));
    assert!(matches!(
        parse_market_event(r#"{"event_type":"best_bid_ask"}"#),
        Ok(MarketEvent::BestBidAsk(_))
    ));
}

#[test]
fn parses_market_lifecycle_events() {
    let event = parse_market_event(
        r#"{"event_type":"new_market","id":"1031769","assets_ids":["yes","no"],"active":true}"#,
    )
    .unwrap();
    assert!(
        matches!(event, MarketEvent::NewMarket(market) if market.asset_ids == ["yes", "no"] && market.active)
    );

    let event = parse_market_event(
        r#"{"event_type":"market_resolved","assets_ids":["yes","no"],"winning_asset_id":"yes","winning_outcome":"Yes"}"#,
    )
    .unwrap();
    assert!(
        matches!(event, MarketEvent::MarketResolved(market) if market.winning_asset_id == "yes" && market.winning_outcome == "Yes")
    );
}
