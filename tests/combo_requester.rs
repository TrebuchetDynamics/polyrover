#![cfg(feature = "execution")]

use polyrover::{
    auth::{sign_hmac, ApiKey, L2Credentials},
    combo_requester::*,
};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
    time::Duration,
};

fn credentials() -> L2Credentials {
    L2Credentials {
        address: "0x1111111111111111111111111111111111111111".into(),
        api_key: ApiKey {
            key: "test-key".into(),
            secret: "c2VjcmV0".into(),
            passphrase: "test-passphrase".into(),
        },
    }
}
fn request() -> CreateRequest {
    CreateRequest {
        signer_address: credentials().address,
        maker_address: "0x2222222222222222222222222222222222222222".into(),
        signature_type: 1,
        leg_position_ids: vec!["123".into(), "456".into()],
        direction: Direction::Buy,
        side: Side::Yes,
        requested_size: RequestedSize {
            unit: SizeUnit::Notional,
            value_e6: "1000000".into(),
        },
    }
}
fn response() -> Value {
    let r = request();
    json!({"rfq_id":"rfq-1","status":"AWAITING_REQUESTER_ACCEPTANCE","expires_at":chrono::Utc::now().timestamp_millis()+60000,"request":{"rfq_id":"rfq-1","maker_address":r.maker_address,"requestor_public_id":"requester","leg_position_ids":r.leg_position_ids,"condition_id":"condition","yes_position_id":"789","no_position_id":"790","direction":"BUY","side":"YES","requested_size":r.requested_size,"created_at":chrono::Utc::now().timestamp_millis()},"quote":{"quote_id":"quote-1","blended_price_e6":"500000","maker_amount_e6":"966191","taker_amount_e6":"1932381","total_required_e6":"1000000","net_receive_e6":"1932381"}})
}
fn order() -> SignedOrder {
    SignedOrder {
        salt: "1".into(),
        maker: request().maker_address,
        signer: request().signer_address,
        token_id: "789".into(),
        maker_amount: "966191".into(),
        taker_amount: "1932381".into(),
        side: 0,
        signature_type: 1,
        timestamp: "1794090000".into(),
        metadata: format!("0x{}", "0".repeat(64)),
        builder: format!("0x{}", "0".repeat(64)),
        signature: format!("0x{}", "ab".repeat(65)),
    }
}
fn header<'a>(r: &'a str, name: &str) -> &'a str {
    r.lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim())
        })
        .unwrap()
}
fn serve(
    replies: Vec<(u16, Value)>,
    delay: Duration,
) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
    let (url, receiver, thread, listener) = serve_with_listener(replies, delay);
    drop(listener);
    (url, receiver, thread)
}

fn serve_with_listener(
    replies: Vec<(u16, Value)>,
    delay: Duration,
) -> (
    String,
    mpsc::Receiver<String>,
    thread::JoinHandle<()>,
    TcpListener,
) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let observer = l.try_clone().unwrap();
    let (tx, rx) = mpsc::channel();
    let h = thread::spawn(move || {
        for (status, body) in replies {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let n = s.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&buf[..n]);
                if let Some(i) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&bytes[..i]);
                    let len = head
                        .lines()
                        .find_map(|v| {
                            let (k, v) = v.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if bytes.len() >= i + 4 + len {
                        break;
                    }
                }
            }
            let _ = tx.send(String::from_utf8(bytes).unwrap());
            thread::sleep(delay);
            let body = body.to_string();
            let _=write!(s,"HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
        }
    });
    (format!("http://{addr}"), rx, h, observer)
}

#[test]
fn request_rejects_wrong_units_duplicates_and_unbound_identities() {
    let c = credentials();
    let mut r = request();
    r.validate(&c).unwrap();
    r.requested_size.unit = SizeUnit::Shares;
    assert!(r.validate(&c).is_err());
    r = request();
    r.leg_position_ids[1] = r.leg_position_ids[0].clone();
    assert!(r.validate(&c).is_err());
    r = request();
    r.signer_address = r.maker_address.clone();
    assert!(r.validate(&c).is_err());
    r.signature_type = 3;
    r.validate(&c).unwrap();
    r.signature_type = 0;
    assert!(r.validate(&c).is_err());
}
#[test]
fn quote_binding_and_expiry_are_required() {
    let r = request();
    let c = credentials();
    let parsed: RfqResponse = serde_json::from_value(response()).unwrap();
    parsed
        .executable_for(&r, &c, chrono::Utc::now().timestamp_millis())
        .unwrap();
    assert!(parsed.executable_for(&r, &c, i64::MAX).is_err());
    for (field, value) in [
        ("rfq_id", json!("other")),
        ("maker_address", json!(c.address)),
        ("leg_position_ids", json!(["456", "123"])),
    ] {
        let mut raw = response();
        raw["request"][field] = value;
        let p: RfqResponse = serde_json::from_value(raw).unwrap();
        assert!(p.executable_for(&r, &c, 0).is_err());
    }
}
#[test]
fn business_failure_and_future_status_are_never_executable() {
    for state in ["FAILED", "FUTURE_PROVIDER_STATE"] {
        let mut raw = response();
        raw["status"] = json!(state);
        let p: RfqResponse = serde_json::from_value(raw).unwrap();
        assert_eq!(p.status.as_str(), state);
        assert!(p.executable_for(&request(), &credentials(), 0).is_err());
        assert!(!p.status.is_success());
    }
    let mut raw = response();
    raw["error"] = json!({"code":"FUTURE_ERROR"});
    let p: RfqResponse = serde_json::from_value(raw).unwrap();
    assert!(p.executable_for(&request(), &credentials(), 0).is_err());
}
#[tokio::test]
async fn exact_wire_body_and_all_three_paths_are_signed() {
    let (mut status, accept) = (
        json!({"rfq_id":"rfq-1","status":"FILLED","tx_hash":"0xtx"}),
        json!({"rfq_id":"rfq-1","status":"EXECUTING"}),
    );
    status["unknown_field"] = json!(true);
    let (url, rx, h) = serve(
        vec![(200, response()), (200, accept), (200, status)],
        Duration::ZERO,
    );
    let client = Client::with_loopback_for_testing(&url, Duration::from_secs(2)).unwrap();
    let c = credentials();
    let created = client.create(&c, &request()).await.unwrap();
    created
        .response()
        .executable_for(&request(), &c, chrono::Utc::now().timestamp_millis())
        .unwrap();
    assert_eq!(
        client
            .accept(&c, &created, &order())
            .await
            .unwrap()
            .status
            .as_str(),
        "EXECUTING"
    );
    assert!(client
        .status(&c, &created)
        .await
        .unwrap()
        .status
        .is_success());
    for (method, path) in [
        ("POST", "/v1/requester/rfq/requests"),
        ("POST", "/v1/requester/rfq/requests/rfq-1/accept"),
        ("GET", "/v1/requester/rfq/requests/rfq-1"),
    ] {
        let raw = rx.recv().unwrap();
        assert!(raw.starts_with(&format!("{method} {path} HTTP/1.1")));
        let body = raw.split_once("\r\n\r\n").unwrap().1;
        let ts = header(&raw, "poly_timestamp").parse().unwrap();
        assert_eq!(
            header(&raw, "poly_signature"),
            sign_hmac(
                &c.api_key.secret,
                ts,
                method,
                path,
                (!body.is_empty()).then_some(body)
            )
        );
        assert_eq!(header(&raw, "poly_address"), c.address);
        if path.ends_with("/accept") {
            let v: Value = serde_json::from_str(body).unwrap();
            assert_eq!(v["quote_id"], "quote-1");
            assert_eq!(v["signed_order"]["tokenId"], "789");
        }
    }
    h.join().unwrap();
}
#[tokio::test]
async fn acceptance_rejects_account_quote_and_order_mix_without_http() {
    let (url, rx, h) = serve(vec![(200, response())], Duration::ZERO);
    let client = Client::with_loopback_for_testing(&url, Duration::from_secs(1)).unwrap();
    let c = credentials();
    let created = client.create(&c, &request()).await.unwrap();
    rx.recv().unwrap();
    h.join().unwrap();
    let mut wrong_token = order();
    wrong_token.token_id = "999".into();
    let mut wrong_amount = order();
    wrong_amount.maker_amount = "100".into();
    let mut attributed = order();
    attributed.builder = format!("0x{}", "1".repeat(64));
    let mut wrong_identity = order();
    wrong_identity.signature_type = 2;
    for o in [wrong_token, wrong_amount, attributed, wrong_identity] {
        assert!(client.accept(&c, &created, &o).await.is_err());
    }
    let mut other = c.clone();
    other.address = request().maker_address;
    assert!(client.accept(&other, &created, &order()).await.is_err());
}
#[tokio::test]
async fn failed_business_response_remains_readable_without_quote() {
    let (url, _, h) = serve(
        vec![(
            200,
            json!({"rfq_id":"rfq-1","status":"FAILED","error":{"code":"NO_QUOTES","message":"no quotes"}}),
        )],
        Duration::ZERO,
    );
    let client = Client::with_loopback_for_testing(&url, Duration::from_secs(1)).unwrap();
    let c = credentials();
    let created = client.create(&c, &request()).await.unwrap();
    assert_eq!(created.response().error.as_ref().unwrap().code, "NO_QUOTES");
    assert!(created
        .response()
        .executable_for(&request(), &c, 0)
        .is_err());
    h.join().unwrap();
}
#[tokio::test]
async fn create_timeout_is_unknown_and_has_no_automatic_retry() {
    let (url, rx, h) = serve(vec![(200, response())], Duration::from_millis(150));
    let client = Client::with_loopback_for_testing(&url, Duration::from_millis(30)).unwrap();
    let err = client.create(&credentials(), &request()).await.unwrap_err();
    assert!(matches!(
        err,
        Error::OutcomeUnknown {
            operation: Operation::Create,
            rfq_id: None
        }
    ));
    assert!(!format!("{err:?}").contains("test-passphrase"));
    rx.recv().unwrap();
    h.join().unwrap();
    assert!(rx.try_recv().is_err());
}
#[test]
fn host_and_secret_validation_fail_closed() {
    for url in [
        "http://example.com",
        "https://127.0.0.1.attacker.test",
        "http://127.0.0.1:123/a",
        "http://user:pass@127.0.0.1:123",
    ] {
        assert!(Client::with_loopback_for_testing(url, Duration::from_secs(1)).is_err());
    }
    let mut c = credentials();
    c.api_key.secret = "not!base64".into();
    assert!(request().validate(&c).is_err());
}

#[tokio::test]
async fn oversized_or_malformed_create_response_is_unknown_not_retryable_success() {
    let mut oversized = response();
    oversized["padding"] = json!("x".repeat(262144));
    for body in [oversized, json!("not an RFQ object")] {
        let (url, rx, h) = serve(vec![(200, body)], Duration::ZERO);
        let client = Client::with_loopback_for_testing(&url, Duration::from_secs(1)).unwrap();
        assert!(matches!(
            client.create(&credentials(), &request()).await.unwrap_err(),
            Error::OutcomeUnknown {
                operation: Operation::Create,
                rfq_id: None
            }
        ));
        rx.recv().unwrap();
        h.join().unwrap();
        assert!(rx.try_recv().is_err());
    }
}
#[tokio::test]
async fn upstream_http_errors_do_not_echo_credentials_or_response_bodies() {
    let (url, _, h) = serve(
        vec![(401, json!({"error":"test-passphrase c2VjcmV0 test-key"}))],
        Duration::ZERO,
    );
    let client = Client::with_loopback_for_testing(&url, Duration::from_secs(1)).unwrap();
    let error = client.create(&credentials(), &request()).await.unwrap_err();
    assert_eq!(error, Error::Http { status: 401 });
    for secret in ["test-passphrase", "c2VjcmV0", "test-key"] {
        assert!(!format!("{error:?} {error}").contains(secret));
    }
    h.join().unwrap();
}
#[tokio::test]
async fn status_recovery_keeps_id_and_authenticates_each_independent_read() {
    let (url, rx, h) = serve(
        vec![
            (200, response()),
            (200, json!({"rfq_id":"wrong-id","status":"FILLED"})),
            (200, json!({"rfq_id":"rfq-1","status":"FUTURE_STATE"})),
            (
                200,
                json!({"rfq_id":"rfq-1","status":"FAILED","error":{"code":"FUTURE_ERROR"}}),
            ),
        ],
        Duration::ZERO,
    );
    let c = credentials();
    let client = Client::with_loopback_for_testing(&url, Duration::from_secs(1)).unwrap();
    let created = client.create(&c, &request()).await.unwrap();
    assert_eq!(
        client.status(&c, &created).await.unwrap_err(),
        Error::OutcomeUnknown {
            operation: Operation::Status,
            rfq_id: Some("rfq-1".into())
        }
    );
    assert_eq!(
        client.status(&c, &created).await.unwrap().status.as_str(),
        "FUTURE_STATE"
    );
    let failed = client.status(&c, &created).await.unwrap();
    assert!(!failed.is_success());
    assert!(failed.status.is_terminal());
    assert_eq!(failed.error.unwrap().code, "FUTURE_ERROR");
    for _ in 0..4 {
        let raw = rx.recv().unwrap();
        assert!(!header(&raw, "poly_timestamp").is_empty());
        assert!(!header(&raw, "poly_signature").is_empty());
    }
    h.join().unwrap();
}
#[tokio::test]
async fn credentials_are_never_forwarded_to_a_redirect_destination() {
    let target = TcpListener::bind("127.0.0.1:0").unwrap();
    target.set_nonblocking(true).unwrap();
    let destination = target.local_addr().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut b = [0; 8192];
        let _ = stream.read(&mut b).unwrap();
        write!(stream,"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{destination}/stolen\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
    });
    let client =
        Client::with_loopback_for_testing(&format!("http://{addr}"), Duration::from_millis(100))
            .unwrap();
    assert_eq!(
        client.create(&credentials(), &request()).await.unwrap_err(),
        Error::Http { status: 307 }
    );
    server.join().unwrap();
    assert_eq!(
        target.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

fn spend_case(direction: Direction) -> (CreateRequest, Value, SignedOrder) {
    let mut request = request();
    let mut response = response();
    let mut order = order();
    if direction == Direction::Sell {
        request.direction = Direction::Sell;
        request.requested_size.unit = SizeUnit::Shares;
        response["request"]["direction"] = json!("SELL");
        response["request"]["requested_size"]["unit"] = json!("shares");
        response["quote"]["maker_amount_e6"] = json!("1000000");
        response["quote"]["taker_amount_e6"] = json!("500000");
        response["quote"]["net_receive_e6"] = json!("480000");
        order.side = 1;
        order.maker_amount = "1000000".into();
        order.taker_amount = "500000".into();
    }
    (request, response, order)
}

fn assert_maker_spend_bound(direction: Direction) {
    let (request, mut raw, _) = spend_case(direction);
    let credentials = credentials();
    let valid: RfqResponse = serde_json::from_value(raw.clone()).unwrap();
    valid
        .executable_for(
            &request,
            &credentials,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();
    raw["quote"]["maker_amount_e6"] = json!("2000000");
    let invalid: RfqResponse = serde_json::from_value(raw).unwrap();
    assert!(
        invalid
            .executable_for(
                &request,
                &credentials,
                chrono::Utc::now().timestamp_millis()
            )
            .is_err(),
        "maker spend exceeds the total required and the requested budget"
    );
}

#[test]
fn buy_maker_spend_cannot_exceed_collateral_budget() {
    assert_maker_spend_bound(Direction::Buy);
}
#[test]
fn sell_maker_spend_cannot_exceed_share_budget() {
    assert_maker_spend_bound(Direction::Sell);
}

async fn assert_budget_rejected_without_http(direction: Direction) {
    let (request, mut raw, mut order) = spend_case(direction);
    raw["quote"]["maker_amount_e6"] = json!("2000000");
    order.maker_amount = "2000000".into();
    let (url, rx, thread, observer) = serve_with_listener(vec![(200, raw)], Duration::ZERO);
    let client = Client::with_loopback_for_testing(&url, Duration::from_millis(100)).unwrap();
    let credentials = credentials();
    let created = client.create(&credentials, &request).await.unwrap();
    rx.recv().unwrap();
    thread.join().unwrap();
    observer.set_nonblocking(true).unwrap();
    let result = client.accept(&credentials, &created, &order).await;
    assert_eq!(
        observer.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock,
        "invalid quote must not initiate another connection"
    );
    assert!(matches!(result, Err(Error::Invalid(_))));
}
#[tokio::test]
async fn buy_overspend_acceptance_is_rejected_before_http() {
    assert_budget_rejected_without_http(Direction::Buy).await;
}
#[tokio::test]
async fn sell_overspend_acceptance_is_rejected_before_http() {
    assert_budget_rejected_without_http(Direction::Sell).await;
}

#[test]
fn trusted_snapshot_roundtrip_restores_without_secrets() {
    let credentials = credentials();
    let original_request = request();
    let original_response: RfqResponse = serde_json::from_value(response()).unwrap();
    let stored =
        serde_json::to_vec(&(&original_request, &original_response, &credentials.address)).unwrap();
    let (request, response, address): (CreateRequest, RfqResponse, String) =
        serde_json::from_slice(&stored).unwrap();
    let restored = CreatedRfq::restore(request, response, address, &credentials).unwrap();
    assert_eq!(restored.response(), &original_response);
    restored
        .validate_order(
            &credentials,
            &order(),
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();
    for secret in [
        &credentials.api_key.key,
        &credentials.api_key.secret,
        &credentials.api_key.passphrase,
    ] {
        assert!(!String::from_utf8_lossy(&stored).contains(secret));
        assert!(!format!("{restored:?}").contains(secret));
    }
}

#[test]
fn restore_rejects_unbound_recorded_identity_and_unsafe_or_mixed_response() {
    let credentials = credentials();
    let parsed: RfqResponse = serde_json::from_value(response()).unwrap();
    assert!(CreatedRfq::restore(
        request(),
        parsed.clone(),
        request().maker_address,
        &credentials
    )
    .is_err());
    let mut other = credentials.clone();
    other.address = request().maker_address;
    assert!(CreatedRfq::restore(request(), parsed, credentials.address.clone(), &other).is_err());
    // Deposit requests do not bind the credential address to the wallet signer;
    // restoration must still bind the separately recorded credential identity.
    let mut deposit_request = request();
    deposit_request.signature_type = 3;
    deposit_request.signer_address = deposit_request.maker_address.clone();
    let deposit_response = serde_json::from_value(response()).unwrap();
    assert!(CreatedRfq::restore(
        deposit_request,
        deposit_response,
        credentials.address.clone(),
        &other
    )
    .is_err());
    let mut cases = Vec::new();
    let mut raw = response();
    raw["rfq_id"] = json!("../other?leak=1");
    cases.push(raw);
    for (field, value) in [
        ("rfq_id", json!("other-rfq")),
        ("maker_address", json!(credentials.address)),
        ("leg_position_ids", json!(["123", "999"])),
        ("direction", json!("SELL")),
        (
            "requested_size",
            json!({"unit":"notional","value_e6":"2000000"}),
        ),
        ("yes_position_id", json!("0789")),
        ("no_position_id", json!("789")),
    ] {
        let mut raw = response();
        raw["request"][field] = value;
        cases.push(raw);
    }
    for raw in cases {
        let parsed = serde_json::from_value(raw).unwrap();
        assert!(
            CreatedRfq::restore(request(), parsed, credentials.address.clone(), &credentials)
                .is_err()
        );
    }
}

#[tokio::test]
async fn expired_restore_supports_status_but_accept_rechecks_real_time_before_http() {
    let credentials = credentials();
    let mut raw = response();
    raw["expires_at"] = json!(1000);
    raw["request"]["created_at"] = json!(0);
    let restored = CreatedRfq::restore(
        request(),
        serde_json::from_value(raw).unwrap(),
        credentials.address.clone(),
        &credentials,
    )
    .unwrap();
    // A successful historical preflight is not an authorization token for accept.
    restored.validate_order(&credentials, &order(), 1).unwrap();
    let (url, rx, thread, observer) = serve_with_listener(
        vec![(200, json!({"rfq_id":"rfq-1","status":"FILLED"}))],
        Duration::ZERO,
    );
    let client = Client::with_loopback_for_testing(&url, Duration::from_millis(100)).unwrap();
    assert!(client
        .status(&credentials, &restored)
        .await
        .unwrap()
        .is_success());
    let request = rx.recv().unwrap();
    assert!(request.starts_with("GET /v1/requester/rfq/requests/rfq-1 HTTP/1.1"));
    thread.join().unwrap();
    observer.set_nonblocking(true).unwrap();
    assert!(matches!(
        client.accept(&credentials, &restored, &order()).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        observer.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn terminal_restore_remains_non_executable_and_preflight_rejects_bad_order_and_budget() {
    let credentials = credentials();
    let terminal: RfqResponse = serde_json::from_value(
        json!({"rfq_id":"rfq-1","status":"FAILED","error":{"code":"NO_QUOTES"}}),
    )
    .unwrap();
    let restored = CreatedRfq::restore(
        request(),
        terminal,
        credentials.address.clone(),
        &credentials,
    )
    .unwrap();
    assert!(restored.response().status.is_terminal());
    assert!(restored.validate_order(&credentials, &order(), 0).is_err());
    let restored = CreatedRfq::restore(
        request(),
        serde_json::from_value(response()).unwrap(),
        credentials.address.clone(),
        &credentials,
    )
    .unwrap();
    let mut bad_order = order();
    bad_order.token_id = "999".into();
    assert!(restored
        .validate_order(
            &credentials,
            &bad_order,
            chrono::Utc::now().timestamp_millis()
        )
        .is_err());
    let mut raw = response();
    raw["quote"]["maker_amount_e6"] = json!("2000000");
    let restored = CreatedRfq::restore(
        request(),
        serde_json::from_value(raw).unwrap(),
        credentials.address.clone(),
        &credentials,
    )
    .unwrap();
    let mut bad_order = order();
    bad_order.maker_amount = "2000000".into();
    assert!(restored
        .validate_order(
            &credentials,
            &bad_order,
            chrono::Utc::now().timestamp_millis()
        )
        .is_err());
    let mut raw = response();
    raw["quote"]["quote_id"] = json!("../different-quote");
    let restored = CreatedRfq::restore(
        request(),
        serde_json::from_value(raw).unwrap(),
        credentials.address.clone(),
        &credentials,
    )
    .unwrap();
    assert!(restored
        .validate_order(
            &credentials,
            &order(),
            chrono::Utc::now().timestamp_millis()
        )
        .is_err());
}

#[tokio::test]
async fn actual_create_snapshot_roundtrip_recovers_status_with_a_new_client() {
    let credentials = credentials();
    let original_request = request();
    let (url, rx, thread) = serve(
        vec![
            (200, response()),
            (200, json!({"rfq_id":"rfq-1","status":"EXECUTING"})),
        ],
        Duration::ZERO,
    );
    let first_client = Client::with_loopback_for_testing(&url, Duration::from_secs(1)).unwrap();
    let created = first_client
        .create(&credentials, &original_request)
        .await
        .unwrap();
    let stored =
        serde_json::to_vec(&(&original_request, created.response(), &credentials.address)).unwrap();
    drop(created);
    drop(first_client);
    let (request, response, address): (CreateRequest, RfqResponse, String) =
        serde_json::from_slice(&stored).unwrap();
    let restored = CreatedRfq::restore(request, response, address, &credentials).unwrap();
    let second_client = Client::with_loopback_for_testing(&url, Duration::from_secs(1)).unwrap();
    assert!(second_client
        .status(&credentials, &restored)
        .await
        .unwrap()
        .status
        .is_pending());
    assert!(rx
        .recv()
        .unwrap()
        .starts_with("POST /v1/requester/rfq/requests HTTP/1.1"));
    assert!(rx
        .recv()
        .unwrap()
        .starts_with("GET /v1/requester/rfq/requests/rfq-1 HTTP/1.1"));
    thread.join().unwrap();
}
