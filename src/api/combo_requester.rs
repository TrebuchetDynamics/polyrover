//! Trusted-infrastructure requester RFQ protocol. No signer, custody or CLI wiring.
//!
//! Calling `create` starts a quote competition; `accept` submits an already signed
//! order. Neither operation retries. An unknown outcome must never be presented as
//! a failed trade or retried create. The returned RFQ handle permits status recovery.

use crate::auth::{build_l2_headers, L2Credentials};
use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt, time::Duration};

pub const DEFAULT_BASE_URL: &str = "https://combos-rfq-gateway-requester-api.polymarket.com";
const REQUESTS: &str = "/v1/requester/rfq/requests";
const MAX_RESPONSE: usize = 262_144;
const UINT256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";

type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Create,
    Accept,
    Status,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    Invalid(&'static str),
    Http {
        status: u16,
    },
    /// Request may have reached the provider. A create has no recoverable ID.
    OutcomeUnknown {
        operation: Operation,
        rfq_id: Option<String>,
    },
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(s) => f.write_str(s),
            Self::Http { status } => write!(f, "RFQ HTTP {status}; upstream body omitted"),
            Self::OutcomeUnknown { operation, rfq_id } => write!(
                f,
                "RFQ {operation:?} outcome unknown; recovery ID: {rfq_id:?}"
            ),
        }
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Direction {
    Buy,
    Sell,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Yes,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SizeUnit {
    Notional,
    Shares,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestedSize {
    pub unit: SizeUnit,
    pub value_e6: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateRequest {
    pub signer_address: String,
    pub maker_address: String,
    pub signature_type: u8,
    pub leg_position_ids: Vec<String>,
    pub direction: Direction,
    pub side: Side,
    pub requested_size: RequestedSize,
}
impl CreateRequest {
    pub fn validate(&self, c: &L2Credentials) -> Result<()> {
        c.validate()
            .map_err(|_| Error::Invalid("invalid L2 credentials"))?;
        if ![
            general_purpose::STANDARD,
            general_purpose::STANDARD_NO_PAD,
            general_purpose::URL_SAFE,
            general_purpose::URL_SAFE_NO_PAD,
        ]
        .iter()
        .any(|e| e.decode(&c.api_key.secret).is_ok_and(|v| !v.is_empty()))
        {
            return Err(Error::Invalid("RFQ requires a base64 CLOB secret"));
        }
        if !address(&self.signer_address) || !address(&self.maker_address) {
            return Err(Error::Invalid("invalid RFQ wallet address"));
        }
        match self.signature_type {
            1 | 2 if same(&self.signer_address, &c.address) => {}
            3 if same(&self.signer_address, &self.maker_address) => {}
            _ => return Err(Error::Invalid("unsupported or unbound RFQ wallet identity")),
        }
        if !(2..=50).contains(&self.leg_position_ids.len())
            || self.leg_position_ids.iter().any(|s| !uint(s))
            || self.leg_position_ids.iter().collect::<HashSet<_>>().len()
                != self.leg_position_ids.len()
        {
            return Err(Error::Invalid(
                "RFQ requires 2 to 50 unique decimal position IDs",
            ));
        }
        if !positive(&self.requested_size.value_e6)
            || !matches!(
                (self.direction, self.requested_size.unit),
                (Direction::Buy, SizeUnit::Notional) | (Direction::Sell, SizeUnit::Shares)
            )
        {
            return Err(Error::Invalid(
                "RFQ size must be positive with the direction's unit",
            ));
        }
        Ok(())
    }
}
/// Unknown provider states are retained and never treated as executable/success.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Status(String);
impl Status {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn is_success(&self) -> bool {
        matches!(self.as_str(), "CONFIRMED" | "FILLED")
    }
    pub fn is_pending(&self) -> bool {
        matches!(
            self.as_str(),
            "AWAITING_MAKER_CONFIRMATION" | "EXECUTING" | "MINED" | "RETRYING"
        )
    }
    pub fn is_terminal(&self) -> bool {
        self.is_success() || matches!(self.as_str(), "FAILED" | "EXPIRED" | "CANCELED")
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BusinessError {
    pub code: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuotedRequest {
    pub rfq_id: String,
    pub maker_address: String,
    pub requestor_public_id: String,
    pub leg_position_ids: Vec<String>,
    pub condition_id: String,
    pub yes_position_id: String,
    pub no_position_id: String,
    pub direction: Direction,
    pub side: Side,
    pub requested_size: RequestedSize,
    pub created_at: i64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Quote {
    pub quote_id: String,
    pub blended_price_e6: String,
    pub maker_amount_e6: String,
    pub taker_amount_e6: String,
    pub total_required_e6: String,
    pub net_receive_e6: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RfqResponse {
    pub rfq_id: String,
    pub status: Status,
    pub expires_at: Option<i64>,
    pub request: Option<QuotedRequest>,
    pub quote: Option<Quote>,
    pub error: Option<BusinessError>,
    pub taker_order_hash: Option<String>,
    pub tx_hash: Option<String>,
}
impl RfqResponse {
    /// A provider success state is usable only without a business error.
    pub fn is_success(&self) -> bool {
        self.status.is_success() && self.error.is_none()
    }
    fn validate_binding(&self, expected: &CreateRequest) -> Result<()> {
        if !identifier(&self.rfq_id) {
            return Err(Error::Invalid("invalid RFQ ID"));
        }
        if let Some(r) = &self.request {
            if r.rfq_id != self.rfq_id
                || !same(&r.maker_address, &expected.maker_address)
                || r.leg_position_ids != expected.leg_position_ids
                || r.direction != expected.direction
                || r.side != expected.side
                || r.requested_size != expected.requested_size
                || !uint(&r.yes_position_id)
                || !uint(&r.no_position_id)
                || r.yes_position_id == r.no_position_id
            {
                return Err(Error::Invalid(
                    "RFQ response does not match the requested account or positions",
                ));
            }
        }
        Ok(())
    }
    pub fn executable_for(
        &self,
        expected: &CreateRequest,
        c: &L2Credentials,
        now_ms: i64,
    ) -> Result<()> {
        expected.validate(c)?;
        if self.status.as_str() != "AWAITING_REQUESTER_ACCEPTANCE" || self.error.is_some() {
            return Err(Error::Invalid("RFQ has no executable quote"));
        }
        let r = self
            .request
            .as_ref()
            .ok_or(Error::Invalid("RFQ request missing"))?;
        let q = self
            .quote
            .as_ref()
            .ok_or(Error::Invalid("RFQ quote missing"))?;
        self.validate_binding(expected)?;
        let expiry = self
            .expires_at
            .ok_or(Error::Invalid("RFQ expiry missing"))?;
        if expiry <= now_ms || r.created_at < 0 || r.created_at >= expiry {
            return Err(Error::Invalid("RFQ quote expired or has invalid times"));
        }
        if !identifier(&q.quote_id)
            || !uint(&q.blended_price_e6)
            || decimal_gt(&q.blended_price_e6, "1000000")
            || [
                &q.maker_amount_e6,
                &q.taker_amount_e6,
                &q.total_required_e6,
                &q.net_receive_e6,
            ]
            .iter()
            .any(|v| !positive(v))
            || decimal_gt(&q.maker_amount_e6, &q.total_required_e6)
            || decimal_gt(&q.total_required_e6, &expected.requested_size.value_e6)
        {
            return Err(Error::Invalid(
                "RFQ quote has invalid amounts or exceeds the requested size",
            ));
        }
        Ok(())
    }
}
/// The caller supplies a previously signed Exchange v3 order. This SDK does not
/// recover signatures or assert the signer's authority on behalf of a user.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedOrder {
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    pub side: u8,
    pub signature_type: u8,
    pub timestamp: String,
    pub metadata: String,
    pub builder: String,
    pub signature: String,
}
impl fmt::Debug for SignedOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignedOrder")
            .field("maker", &self.maker)
            .field("signature", &"<redacted>")
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug)]
pub struct CreatedRfq {
    response: RfqResponse,
    request: CreateRequest,
    credential_address: String,
}
impl CreatedRfq {
    /// Rebuild a handle from trusted server storage, never arbitrary client input.
    /// Expired and terminal records remain recoverable through status; this does
    /// not grant executability or establish wallet ownership/authorization.
    pub fn restore(
        request: CreateRequest,
        response: RfqResponse,
        credential_address: String,
        credentials: &L2Credentials,
    ) -> Result<Self> {
        request.validate(credentials)?;
        if !address(&credential_address) || !same(&credential_address, &credentials.address) {
            return Err(Error::Invalid(
                "RFQ snapshot belongs to a different authenticated account",
            ));
        }
        response.validate_binding(&request)?;
        Ok(Self {
            request,
            response,
            credential_address,
        })
    }

    /// Pure preflight for a durable gateway before reserving an order hash.
    /// `accept` independently repeats these checks with its actual current time.
    pub fn validate_order(
        &self,
        c: &L2Credentials,
        order: &SignedOrder,
        now_ms: i64,
    ) -> Result<()> {
        check_handle(c, self)?;
        self.response.executable_for(&self.request, c, now_ms)?;
        let q = self
            .response
            .quote
            .as_ref()
            .ok_or(Error::Invalid("RFQ quote missing"))?;
        let r = self
            .response
            .request
            .as_ref()
            .ok_or(Error::Invalid("RFQ request missing"))?;
        if !same(&order.maker, &self.request.maker_address)
            || !same(&order.signer, &self.request.signer_address)
            || order.signature_type != self.request.signature_type
            || order.token_id != r.yes_position_id
            || order.maker_amount != q.maker_amount_e6
            || order.taker_amount != q.taker_amount_e6
            || order.side != u8::from(self.request.direction == Direction::Sell)
            || !uint(&order.salt)
            || !positive(&order.timestamp)
            || !hex_bytes(&order.metadata, 32)
            || order.builder != format!("0x{}", "0".repeat(64))
            || !signature(&order.signature, order.signature_type)
        {
            return Err(Error::Invalid("signed order does not match the RFQ quote"));
        }
        Ok(())
    }

    pub fn response(&self) -> &RfqResponse {
        &self.response
    }
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base: reqwest::Url,
}
impl Client {
    pub fn new() -> Result<Self> {
        Self::build(DEFAULT_BASE_URL, Duration::from_secs(15), false)
    }
    /// Explicit test-only destination: a literal loopback IP with no path or credentials.
    pub fn with_loopback_for_testing(base: &str, timeout: Duration) -> Result<Self> {
        Self::build(base, timeout, true)
    }
    fn build(base: &str, timeout: Duration, loopback: bool) -> Result<Self> {
        let url = reqwest::Url::parse(base).map_err(|_| Error::Invalid("invalid RFQ endpoint"))?;
        let valid_host = if loopback {
            url.host_str()
                .and_then(|v| v.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok())
                .is_some_and(|v| v.is_loopback())
                && url.scheme() == "http"
        } else {
            base == DEFAULT_BASE_URL
        };
        if !valid_host
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
            || timeout.is_zero()
            || timeout > Duration::from_secs(120)
        {
            return Err(Error::Invalid("RFQ endpoint or deadline is not allowed"));
        }
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(timeout)
            .build()
            .map_err(|_| Error::Invalid("could not construct RFQ transport"))?;
        Ok(Self { http, base: url })
    }
    /// Starts one competition. No retry or idempotency is implied.
    pub async fn create(&self, c: &L2Credentials, request: &CreateRequest) -> Result<CreatedRfq> {
        request.validate(c)?;
        let body = serde_json::to_vec(request)
            .map_err(|_| Error::Invalid("could not encode RFQ request"))?;
        let response = self
            .send(c, Operation::Create, REQUESTS, Some(body), None)
            .await?;
        if !identifier(&response.rfq_id) {
            return Err(unknown(Operation::Create, None));
        }
        Ok(CreatedRfq {
            response,
            request: request.clone(),
            credential_address: c.address.clone(),
        })
    }
    /// Validates an exact returned quote, then submits the supplied signature once.
    pub async fn accept(
        &self,
        c: &L2Credentials,
        rfq: &CreatedRfq,
        order: &SignedOrder,
    ) -> Result<RfqResponse> {
        rfq.validate_order(c, order, Utc::now().timestamp_millis())?;
        let q = rfq
            .response
            .quote
            .as_ref()
            .ok_or(Error::Invalid("RFQ quote missing"))?;
        #[derive(Serialize)]
        struct Acceptance<'a> {
            quote_id: &'a str,
            signed_order: &'a SignedOrder,
        }
        let body = serde_json::to_vec(&Acceptance {
            quote_id: &q.quote_id,
            signed_order: order,
        })
        .map_err(|_| Error::Invalid("could not encode RFQ acceptance"))?;
        let path = format!("{REQUESTS}/{}/accept", rfq.response.rfq_id);
        self.send(
            c,
            Operation::Accept,
            &path,
            Some(body),
            Some(&rfq.response.rfq_id),
        )
        .await
    }
    /// One newly authenticated status read. Poll policy belongs to the caller;
    /// pending/future states and transport timeouts are not failed trades.
    pub async fn status(&self, c: &L2Credentials, rfq: &CreatedRfq) -> Result<RfqResponse> {
        check_handle(c, rfq)?;
        self.send(
            c,
            Operation::Status,
            &format!("{REQUESTS}/{}", rfq.response.rfq_id),
            None,
            Some(&rfq.response.rfq_id),
        )
        .await
    }
    async fn send(
        &self,
        c: &L2Credentials,
        operation: Operation,
        path: &str,
        body: Option<Vec<u8>>,
        id: Option<&str>,
    ) -> Result<RfqResponse> {
        let method = if operation == Operation::Status {
            "GET"
        } else {
            "POST"
        };
        let text = body
            .as_ref()
            .map(|b| std::str::from_utf8(b).expect("JSON is UTF-8"));
        let headers = build_l2_headers(c, Utc::now().timestamp(), method, path, text)
            .map_err(|_| Error::Invalid("invalid RFQ credentials"))?;
        let url = self
            .base
            .join(path)
            .map_err(|_| Error::Invalid("invalid RFQ path"))?;
        let mut req = self.http.request(
            if method == "GET" {
                reqwest::Method::GET
            } else {
                reqwest::Method::POST
            },
            url,
        );
        for (k, v) in headers {
            req = req.header(k, v);
        }
        if let Some(b) = body {
            req = req
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(b);
        }
        let mut response = req.send().await.map_err(|_| unknown(operation, id))?;
        let status = response.status();
        if !status.is_success() {
            return Err(
                if status.is_server_error() && operation != Operation::Status {
                    unknown(operation, id)
                } else {
                    Error::Http {
                        status: status.as_u16(),
                    }
                },
            );
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_RESPONSE as u64)
        {
            return Err(unknown(operation, id));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unknown(operation, id))? {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE {
                return Err(unknown(operation, id));
            }
            bytes.extend_from_slice(&chunk);
        }
        let parsed: RfqResponse =
            serde_json::from_slice(&bytes).map_err(|_| unknown(operation, id))?;
        if !identifier(&parsed.rfq_id) || id.is_some_and(|expected| expected != parsed.rfq_id) {
            return Err(unknown(operation, id));
        }
        Ok(parsed)
    }
}
fn check_handle(c: &L2Credentials, r: &CreatedRfq) -> Result<()> {
    r.request.validate(c)?;
    if !same(&c.address, &r.credential_address) {
        return Err(Error::Invalid(
            "RFQ belongs to a different authenticated account",
        ));
    }
    Ok(())
}
fn unknown(operation: Operation, id: Option<&str>) -> Error {
    Error::OutcomeUnknown {
        operation,
        rfq_id: id.map(str::to_owned),
    }
}
fn address(v: &str) -> bool {
    hex_bytes(v, 20)
}
fn same(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}
fn hex_bytes(v: &str, n: usize) -> bool {
    v.strip_prefix("0x")
        .is_some_and(|s| s.len() == n * 2 && s.bytes().all(|c| c.is_ascii_hexdigit()))
}
fn signature(v: &str, t: u8) -> bool {
    v.strip_prefix("0x").is_some_and(|s| {
        s.len() % 2 == 0
            && s.bytes().all(|c| c.is_ascii_hexdigit())
            && if t == 3 {
                s.len() > 130 && s.len() <= 32768
            } else {
                s.len() == 130
            }
    })
}
fn uint(v: &str) -> bool {
    !v.is_empty()
        && v.bytes().all(|b| b.is_ascii_digit())
        && (v == "0" || !v.starts_with('0'))
        && v.len() <= UINT256_MAX.len()
        && (v.len() < UINT256_MAX.len() || v <= UINT256_MAX)
}
fn positive(v: &str) -> bool {
    uint(v) && v != "0"
}
fn decimal_gt(a: &str, b: &str) -> bool {
    a.len() > b.len() || (a.len() == b.len() && a > b)
}

fn identifier(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 128
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
